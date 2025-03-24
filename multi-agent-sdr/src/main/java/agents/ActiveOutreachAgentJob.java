/**
 * ActiveOutreachAgentJob is a Flink streaming job that listens for high-quality leads from a Kafka topic,
 * filters those flagged for active engagement, and asynchronously generates personalized outreach emails using OpenAI.
 *
 * <p>Key Responsibilities:
 * <ul>
 *   <li>Consumes scored leads from the "lead_scoring_output" Kafka topic.</li>
 *   <li>Filters for leads with a next_step of "Actively Engage".</li>
 *   <li>Uses the OpenAI API (GPT-4 Turbo) to generate highly personalized email campaigns tailored to the lead's role,
 *       company, and industry context.</li>
 *   <li>Enriches prompt context via tools like Salesforce, Clearbit, LinkedIn, and company website data.</li>
 *   <li>Publishes the generated emails as JSON to the "email_campaigns" Kafka topic for downstream execution.</li>
 * </ul>
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
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
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

public class ActiveOutreachAgentJob {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public static final String SYSTEM_PROMPT = """
      You're the AI Email Engagement Specialist at StratusDB, a cloud-native, AI-powered data warehouse built for B2B
      enterprises that need fast, scalable, and intelligent data infrastructure. StratusDB simplifies complex data
      pipelines, enabling companies to store, query, and operationalize their data in real time.

      You craft engaging, high-converting emails that capture attention, drive conversations, and move leads forward.
      Your messaging is personalized, data-driven, and aligned with industry pain points to ensure relevance and impact.

      Your role is to write compelling outreach emails, optimize engagement through A/B testing and behavioral insights,
      and ensure messaging resonates with each prospect's needs and challenges.
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

    KafkaSource<String> scoredLeadsSource = KafkaSource.<String>builder()
        .setProperties(consumerConfig)
        .setTopics("lead_scoring_output")
        .setStartingOffsets(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

    DataStream<String> scoredLeadsStream = env
        .fromSource(scoredLeadsSource, WatermarkStrategy.noWatermarks(), "scored_leads_source");

    // Only process the leads marked as "actively engage"
    DataStream<String> leadToActivelyEngageStream = scoredLeadsStream
        .filter(record -> {
          try {
            JsonNode node = objectMapper.readTree(AgentTools.cleanJsonString(record));
            return node.has("lead_evaluation")
                && node.get("lead_evaluation").has("next_step")
                && node.get("lead_evaluation").get("next_step").textValue().equals("Actively Engage");
          } catch (Exception e) {
            e.printStackTrace();
            // Log or ignore malformed input
            return false;
          }
        });

    // Apply Async active lead campaign
    DataStream<String> activeEngageStream = AsyncDataStream.unorderedWait(
        leadToActivelyEngageStream,
        new ActiveOutreachFunction(apiKey),
        90, // Timeout
        TimeUnit.SECONDS,
        10 // Max concurrent async requests
    );

    KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
        .setKafkaProducerConfig(producerConfig)
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic("email_campaigns")
            .setValueSerializationSchema(new SimpleStringSchema())
            .build())
        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();

    activeEngageStream.sinkTo(kafkaSink);

    env.execute();
  }

  /**
   * Async Function to build an active engage campaign for the lead.
   */
  public static class ActiveOutreachFunction implements AsyncFunction<String, String> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // Pattern to remove non-printable ASCII and non-UTF characters
    private static final Pattern INVALID_CHARS = Pattern.compile("[^\\x20-\\x7E]");

    private final String apiKey;

    public ActiveOutreachFunction(String apiKey) {
      this.apiKey = apiKey;
    }

    @Override
    public void asyncInvoke(String leadJson, ResultFuture<String> resultFuture) {
      CompletableFuture.supplyAsync(() -> {
        try {
          // Clean JSON string (remove non-printable characters)
          String cleanedJson = AgentTools.cleanJsonString(leadJson);

          JsonNode rootNode = objectMapper.readTree(cleanedJson);

          // Add score to JSON
          ObjectNode leadDataNode = (ObjectNode) rootNode.get("lead_data");
          ObjectNode leadEvaluationNode = (ObjectNode) rootNode.get("lead_evaluation");

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
              .addTool(ChatCompletionTool.builder()
                  .function(FunctionDefinition.builder()
                      .name("getRecentLinkedInPosts")
                      .description("Gathers recent activity and mutual connections from LinkedIn to inform messaging.")
                      .parameters(FunctionParameters.builder()
                          .putAdditionalProperty("type", JsonValue.from("object"))
                          .putAdditionalProperty(
                              "properties", JsonValue.from(Map.of(
                                  "email", Map.of("type", "string"),
                                  "job_title", Map.of("type", "string"),
                                  "name", Map.of("type", "string"))))
                          .putAdditionalProperty("required",
                              JsonValue.from(List.of("email", "job_title", "name")))
                          .putAdditionalProperty("additionalProperties", JsonValue.from(false))
                          .build())
                      .build())
                  .build())
              .addDeveloperMessage(SYSTEM_PROMPT)
              .addUserMessage(buildPrompt(objectMapper.writeValueAsString(leadDataNode),
                  objectMapper.writeValueAsString(leadEvaluationNode),
                  Constants.PRODUCT_DESCRIPTION));

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
          createParamsBuilder.addUserMessage(
              buildPrompt(objectMapper.writeValueAsString(leadDataNode),
                  objectMapper.writeValueAsString(leadEvaluationNode),
                  Constants.PRODUCT_DESCRIPTION));

          ChatCompletion chatCompletion = client.chat().completions().create(createParamsBuilder.build());
          String responseContent = chatCompletion.choices().stream()
              .map(choice -> choice.message().content().orElse(""))
              .collect(Collectors.joining("\n"));

          String campaignType = leadEvaluationNode.get("next_step").asText();
          JsonNode email = objectMapper.readTree(responseContent);

          ObjectNode campaign = objectMapper.createObjectNode();
          campaign.put("campaign_type", campaignType);

          ArrayNode emails = objectMapper.createArrayNode();
          emails.add(email);

          campaign.set("emails", emails);

          // Convert back to JSON string
          return objectMapper.writeValueAsString(campaign);
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

    private static String buildPrompt(String leadDetails, String context, String productDescription) {
      return """
           Using the lead input and evaluation data to craft a highly personalized and engaging email to initiate a conversation with the prospect.
           The email should be tailored to their industry, role, and business needs, ensuring relevance and increasing the likelihood of a response.

           Key Responsibilities:
           - Personalize outreach based on lead insights from company website, LinkedIn, Salesforce, and Clearbit.
           - Craft a compelling email structure, ensuring clarity, relevance, and engagement.
           - Align messaging with the prospect's pain points and industry trends, showing how StratusAI Warehouse addresses their challenges.

           Use dedicated tools to enhance personalization and optimize engagement:
           - Company Website Lookup Tool - Extracts relevant company details, recent news, and strategic initiatives.
           - Salesforce Data Access - Retrieves CRM data about the lead 's past interactions, engagement status, and any prior outreach.
           - Clearbit Enrichment API - Provides firmographic and contact-level data, including company size, funding, tech stack, and key decision-makers.
           - LinkedIn Profile API - Gathers recent activity and mutual connections to inform messaging.

           Ensure a clear and actionable CTA, encouraging the lead to engage without high friction.

           Lead Data
           - Lead Form Responses: %s
           - Lead Evaluation: %s

          %s

           Expected Output - Personalized Prospect Email:
           The email should be concise, engaging, and structured to drive a response, containing:

           - Personalized Opening - Address the lead by name and reference a relevant insight from their company, role, or industry trends.
           - Key Challenge & Value Proposition - Identify a pain point or opportunity based on lead data and explain how StratusAI Warehouse solves it.
           - Clear Call to Action (CTA) - Encourage a response with a low-friction action, such as scheduling a quick chat or sharing feedback.
           - Engagement-Oriented Tone - Maintain a conversational yet professional approach, keeping the message brief and impactful.

           Output Format
           - The output must be strictly formatted as JSON, with no additional text, commentary, or explanation.
           - The JSON should exactly match the following structure:
             {
                 "to": "Lead's Email Address",
                 "subject": "Example Subject Line",
                 "body": "Example Email Body"
             }

           Failure to strictly follow this format will result in incorrect output.
             """
          .formatted(leadDetails, context, productDescription);
    }

    private static String callFunction(ChatCompletionMessageToolCall.Function function) {
      if (function.name().equals("getCompanyWebsite")) {
        return AgentTools.getCompanyWebsite(function);
      } else if (function.name().equals("getSalesforceData")) {
        return AgentTools.getSalesforceData(function);
      } else if (function.name().equals("getClearbitData")) {
        return AgentTools.getClearbitData(function);
      } else if (function.name().equals("getRecentLinkedInPosts")) {
        return AgentTools.getRecentLinkedInPosts(function);
      }

      throw new IllegalArgumentException("Unknown function: " + function.name());
    }
  }
}
