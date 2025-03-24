/**
 * LeadIngestionAgentJob is a Flink streaming job that performs asynchronous research on incoming leads
 * to enrich them with contextual insights for downstream scoring and engagement workflows.
 *
 * <p>Key Responsibilities:
 * <ul>
 *   <li>Consumes raw lead submissions from the "incoming_leads" Kafka topic.</li>
 *   <li>Asynchronously enriches each lead using GPT-4 Turbo, guided by a structured research prompt.</li>
 *   <li>Incorporates auxiliary data via tools including Salesforce, Clearbit, and the lead's company website.</li>
 *   <li>Produces a research summary and lead context JSON to the "lead_ingestion_output" Kafka topic.</li>
 * </ul>
 *
 * <p>This job acts as the first step in the AI-driven sales funnel, automating research and segmentation
 * to support lead scoring, prioritization, and personalized outreach.
 */

package agents;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.kafka.clients.producer.ProducerConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.github.cdimascio.dotenv.Dotenv;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.ChatModel;
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionTool;
import com.openai.models.chat.completions.ChatCompletionToolMessageParam;
import com.openai.models.chat.completions.ChatCompletionMessageToolCall;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import static com.openai.core.ObjectMappers.jsonMapper;
import com.openai.models.FunctionDefinition;
import com.openai.models.FunctionParameters;
import com.openai.core.JsonObject;
import com.openai.core.JsonValue;
import com.openai.models.responses.Response;
import com.openai.models.responses.ResponseCreateParams;
import com.fasterxml.jackson.core.JsonProcessingException;

import tools.AgentTools;
import util.Constants;

import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class LeadIngestionAgentJob {
  public static final String SYSTEM_PROMPT = """
      You're an Industry Research Specialist at StratusDB, a cloud-native, AI-powered data warehouse built for B2B
      enterprises that need fast, scalable, and intelligent data infrastructure. StratusDB simplifies complex data
      pipelines, enabling companies to store, query, and operationalize their data in real time.

      Your role is to conduct research on potential leads to assess their fit for StratusAI Warehouse and provide key
      insights for scoring and outreach planning. Your research will focus on industry trends, company background,
      and AI adoption potential to ensure a tailored and strategic approach.
      """;

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
    String apiKey = dotenv.get("OPENAI_API_KEY");

    Properties consumerConfig = new Properties();
    try (InputStream stream = LeadIngestionAgentJob.class.getClassLoader().getResourceAsStream("consumer.properties")) {
      consumerConfig.load(stream);
    }

    Properties producerConfig = new Properties();
    try (InputStream stream = LeadIngestionAgentJob.class.getClassLoader().getResourceAsStream("producer.properties")) {
      producerConfig.load(stream);
    }

    KafkaSource<String> incomingLeadsSource = KafkaSource.<String>builder()
        .setProperties(consumerConfig)
        .setTopics("incoming_leads")
        .setStartingOffsets(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

    DataStream<String> incomingLeadsStream = env
        .fromSource(incomingLeadsSource, WatermarkStrategy.noWatermarks(), "incoming_leads_source");

    incomingLeadsStream.print();

    // Apply Async lead research
    DataStream<String> leadResearchStream = AsyncDataStream.unorderedWait(
        incomingLeadsStream,
        new LeadResearchFunction(apiKey),
        30, // Timeout
        TimeUnit.SECONDS,
        10 // Max concurrent async requests
    );

    KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
        .setKafkaProducerConfig(producerConfig)
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic("lead_ingestion_output")
            .setValueSerializationSchema(new SimpleStringSchema())
            .build())
        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();

    leadResearchStream.sinkTo(kafkaSink);

    env.execute();
  }

  /**
   * Async Function to research and assess a lead.
   */
  public static class LeadResearchFunction implements AsyncFunction<String, String> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // Pattern to remove non-printable ASCII and non-UTF characters
    private static final Pattern INVALID_CHARS = Pattern.compile("[^\\x20-\\x7E]");

    private final String apiKey;

    public LeadResearchFunction(String apiKey) {
      this.apiKey = apiKey;
    }

    @Override
    public void asyncInvoke(String leadJson, ResultFuture<String> resultFuture) {
      CompletableFuture.supplyAsync(() -> {
        try {
          // Clean JSON string (remove non-printable characters)
          String cleanedJson = AgentTools.cleanJsonString(leadJson);

          // Parse JSON
          JsonNode rootNode = objectMapper.readTree(cleanedJson);
          if (!rootNode.has("lead_data")) {
            return leadJson; // If lead_data field is missing, return original JSON
          }
          // Add score to JSON
          ObjectNode leadDataNode = (ObjectNode) rootNode.get("lead_data");

          OpenAIClient client = OpenAIOkHttpClient.builder()
              .apiKey(this.apiKey)
              .build();

          ChatCompletionCreateParams.Builder createParamsBuilder = ChatCompletionCreateParams.builder()
              .model(ChatModel.GPT_4_TURBO)
              .maxCompletionTokens(2048)
              .addTool(ChatCompletionTool.builder()
                  .function(FunctionDefinition.builder()
                      .name("getSalesforceData")
                      .description(
                          "Retrieves CRM data about the lead's past interactions, status, and engagement history.")
                      .parameters(FunctionParameters.builder()
                          .putAdditionalProperty("type", JsonValue.from("object"))
                          .putAdditionalProperty(
                              "properties", JsonValue.from(Map.of(
                                  "email", Map.of("type", "string"),
                                  "company_website", Map.of("type", "string"),
                                  "company_name", Map.of("type", "string"),
                                  "job_title", Map.of("type", "string"),
                                  "name", Map.of("type", "string"))))
                          .putAdditionalProperty("required",
                              JsonValue.from(List.of("email", "company_website", "company_name", "job_title", "name")))
                          .putAdditionalProperty("additionalProperties", JsonValue.from(false))
                          .build())
                      .build())
                  .build())
              .addTool(ChatCompletionTool.builder()
                  .function(FunctionDefinition.builder()
                      .name("getClearbitData")
                      .description("Retrieves enriched lead data, including both person and company details.")
                      .parameters(FunctionParameters.builder()
                          .putAdditionalProperty("type", JsonValue.from("object"))
                          .putAdditionalProperty(
                              "properties", JsonValue.from(Map.of(
                                  "email", Map.of("type", "string"),
                                  "company_website", Map.of("type", "string"),
                                  "company_name", Map.of("type", "string"),
                                  "job_title", Map.of("type", "string"),
                                  "name", Map.of("type", "string"))))
                          .putAdditionalProperty("required",
                              JsonValue.from(List.of("email", "company_website", "company_name", "job_title", "name")))
                          .putAdditionalProperty("additionalProperties", JsonValue.from(false))
                          .build())
                      .build())
                  .build())
              .addTool(ChatCompletionTool.builder()
                  .function(FunctionDefinition.builder()
                      .name("getCompanyWebsite")
                      .description("Gets the company website page details.")
                      .parameters(FunctionParameters.builder()
                          .putAdditionalProperty("type", JsonValue.from("object"))
                          .putAdditionalProperty(
                              "properties", JsonValue.from(Map.of("url", Map.of("type", "string"))))
                          .putAdditionalProperty("required", JsonValue.from(List.of("url")))
                          .putAdditionalProperty("additionalProperties", JsonValue.from(false))
                          .build())
                      .build())
                  .build())
              .addDeveloperMessage(SYSTEM_PROMPT)
              .addUserMessage(buildPrompt(leadJson, Constants.PRODUCT_DESCRIPTION));

          client.chat().completions().create(createParamsBuilder.build()).choices().stream()
              .map(ChatCompletion.Choice::message)
              // Add each assistant message onto the builder so that we keep track of the
              // conversation for asking a
              // follow-up question later.
              .peek(createParamsBuilder::addMessage)
              .flatMap(message -> {
                message.content().ifPresent(System.out::println);
                return message.toolCalls().stream().flatMap(Collection::stream);
              })
              .forEach(toolCall -> {
                String content = callFunction(toolCall.function());
                // Add the tool call result to the conversation.
                createParamsBuilder.addMessage(ChatCompletionToolMessageParam.builder()
                    .toolCallId(toolCall.id())
                    .content(content)
                    .build());
                System.out.println(content);
              });
          System.out.println();

          // Ask a follow-up once the function calls are complete
          createParamsBuilder.addUserMessage(buildPrompt(leadJson, Constants.PRODUCT_DESCRIPTION));
          ChatCompletion chatCompletion = client.chat().completions().create(createParamsBuilder.build());
          String responseContent = chatCompletion.choices().stream()
              .map(choice -> choice.message().content().orElse(""))
              .collect(Collectors.joining("\n"));

          ((ObjectNode) rootNode).put("context", responseContent);

          System.out.println("Full response:\n" + responseContent);

          System.out.println("Root:");
          System.out.println(rootNode);

          // Convert back to JSON string
          return objectMapper.writeValueAsString(rootNode);
        } catch (Exception e) {
          e.printStackTrace();
          return leadJson; // In case of failure, return original JSON
        }
      }).thenAccept(result -> resultFuture.complete(Collections.singletonList(result)));
    }

    /**
     * Cleans the input JSON string by removing non-printable and invalid
     * characters.
     */
    private String cleanJsonString(String json) {
      // Removes characters outside printable ASCII range
      return INVALID_CHARS.matcher(json).replaceAll("");
    }

    private static String buildPrompt(String leadDetails, String productDescription) {
      return """
          Using the lead input data, conduct preliminary research on the lead. Focus on finding relevant data
          that can aid in scoring the lead and planning a strategy to pitch them. You do not need to score the lead.

          Key Responsibilities:
            - Analyze the lead's industry to identify relevant trends, market challenges, and AI adoption patterns.
            - Gather company-specific insights, including size, market position, recent news, and strategic initiatives.
            - Determine potential use cases for StratusAI Warehouse, focusing on how the company could benefit from real-time analytics, multi-cloud data management, and AI-driven optimization.
            - Assess lead quality based on data completeness and engagement signals. Leads with short or vague form responses should be flagged for review but not immediately discarded.
            - Use dedicated tools to enhance research and minimize manual work:
              - Salesforce Data Access - Retrieves CRM data about the lead's past interactions, status, and engagement history.
              - Company Website Lookup Tool - Fetches key details from the company's official website.
              - Clearbit Enrichment API - Provides firmographic and contact-level data, including company size, funding, tech stack, and key decision-makers.
            - Filter out weak leads or where the lead data doesn't look like a fit, ensuring minimal time is spent on companies unlikely to be a fit for StratusDB's offering.

          Lead Form Responses:
            %s

          %s

          Expected Output - Research Report:
          The research report should be concise and actionable, containing:

          Industry Overview - Key trends, challenges, and AI adoption patterns in the lead's industry.
          Company Insights - Size, market position, strategic direction, and recent news.
          Potential Use Cases - How StratusAI Warehouse could provide value to the lead's company.
          Lead Quality Assessment - Based on available data, engagement signals, and fit for StratusDB's ideal customer profile.
          Additional Insights - Any relevant information that can aid in outreach planning or lead prioritization.
          """
          .formatted(leadDetails, productDescription);
    }

    private static String callFunction(ChatCompletionMessageToolCall.Function function) {
      if (function.name().equals("getCompanyWebsite")) {
        return AgentTools.getCompanyWebsite(function);

      } else if (function.name().equals("getSalesforceData")) {
        return AgentTools.getSalesforceData(function);
      } else if (function.name().equals("getClearbitData")) {
        return AgentTools.getClearbitData(function);
      }

      throw new IllegalArgumentException("Unknown function: " + function.name());
    }
  }
}
