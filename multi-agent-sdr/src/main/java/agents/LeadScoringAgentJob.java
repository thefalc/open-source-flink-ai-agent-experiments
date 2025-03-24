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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.github.cdimascio.dotenv.Dotenv;

import tools.AgentTools;

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

import agents.LeadIngestionAgentJob;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class LeadScoringAgentJob {
  public static final String SYSTEM_PROMPT = """
      You're the Lead Scoring and Strategic Planner at StratusDB, a cloud-native, AI-powered data warehouse built for B2B
      enterprises that need fast, scalable, and intelligent data infrastructure. StratusDB simplifies complex data
      pipelines, enabling companies to store, query, and operationalize their data in real time.

      You combine insights from lead analysis and research to score leads accurately and align them with the
      optimal offering. Your strategic vision and scoring expertise ensure that
      potential leads are matched with solutions that meet their specific needs.

      You role is to utilize analyzed data and research findings to score leads, suggest next steps, and identify talking points.
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

    KafkaSource<String> incomingIngestionSource = KafkaSource.<String>builder()
        .setProperties(consumerConfig)
        .setTopics("lead_ingestion_output")
        .setStartingOffsets(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

    DataStream<String> leadIngestionStream = env
        .fromSource(incomingIngestionSource, WatermarkStrategy.noWatermarks(), "incoming_ingestion_source");

    leadIngestionStream.print();

    // Apply Async Scoring
    DataStream<String> leadScoringStream = AsyncDataStream.unorderedWait(
        leadIngestionStream,
        new LeadScoringFunction(apiKey),
        30, // Timeout
        TimeUnit.SECONDS,
        10 // Max concurrent async requests
    );

    KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
        .setKafkaProducerConfig(producerConfig)
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic("lead_scoring_output")
            .setValueSerializationSchema(new SimpleStringSchema())
            .build())
        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();

    leadScoringStream.sinkTo(kafkaSink);

    env.execute();
  }

  /**
   * Async Function to score the lead.
   */
  public static class LeadScoringFunction implements AsyncFunction<String, String> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // Pattern to remove non-printable ASCII and non-UTF characters
    private static final Pattern INVALID_CHARS = Pattern.compile("[^\\x20-\\x7E]");

    private final String apiKey;

    public LeadScoringFunction(String apiKey) {
      this.apiKey = apiKey;
    }

    @Override
    public void asyncInvoke(String leadIngestionJson, ResultFuture<String> resultFuture) {
      CompletableFuture.supplyAsync(() -> {
        try {
          // Clean JSON string (remove non-printable characters)
          String cleanedJson = AgentTools.cleanJsonString(leadIngestionJson);

          // Parse JSON
          JsonNode rootNode = objectMapper.readTree(cleanedJson);
          if (!rootNode.has("lead_data")) {
            return leadIngestionJson; // If lead_data field is missing, return original JSON
          }

          // Add score to JSON
          ObjectNode leadDataNode = (ObjectNode) rootNode.get("lead_data");
          String contextRaw = rootNode.get("context").textValue();

          OpenAIClient client = OpenAIOkHttpClient.builder()
              .apiKey(this.apiKey)
              .build();

          ChatCompletionCreateParams.Builder createParamsBuilder = ChatCompletionCreateParams.builder()
              .model(ChatModel.GPT_4_TURBO)
              .maxCompletionTokens(2048)
              .addDeveloperMessage(SYSTEM_PROMPT)
              .addUserMessage(buildPrompt(objectMapper.writeValueAsString(leadDataNode), contextRaw));

          ChatCompletion chatCompletion = client.chat().completions().create(createParamsBuilder.build());
          String responseContent = chatCompletion.choices().stream()
              .map(choice -> choice.message().content().orElse(""))
              .collect(Collectors.joining("\n"));

          System.out.println("Full response:\n" + responseContent);

          JsonNode leadEvaluation = objectMapper.readTree(responseContent);

          ObjectNode result = objectMapper.createObjectNode();
          result.set("lead_data", leadDataNode);
          result.set("lead_evaluation", leadEvaluation);

          // Convert back to JSON string
          return objectMapper.writeValueAsString(result);
        } catch (Exception e) {
          e.printStackTrace();
          return leadIngestionJson; // In case of failure, return original JSON
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

    private static String buildPrompt(String leadDetails, String context) {
      return """
            Utilize the provided context and the lead's form response to score the lead.

          - Consider factors such as industry relevance, company size, StratusAI Warehouse use case potential, and buying readiness.
          - Evaluate the wording and length of the response—short answers are a yellow flag.
          - Take into account he role of the lead. Only prioritize leads that fit our core buyer persona. Nurture low quality.
          - Be pessimistic: focus high scores on leads with clear potential to close.
          - Smaller companies typically have lower budgets.
          - Avoid spending too much time on leads that are not a good fit.

          Lead Data
          - Lead Form Responses: %s
          - Additional Context: %s

          Output Format
          - The output must be strictly formatted as JSON, with no additional text, commentary, or explanation.
          - The JSON should exactly match the following structure:
            {
               "score": "80",
               "next_step": "Nurture | Actively Engage",
               "talking_points": "Here are the talking points to engage the lead"
           }

          Formatting Rules
            1. score: An integer between 0 and 100.
            2. next_step: Either "Nurture" or "Actively Engage" (no variations).
            3. talking_points: A list of at least three specific talking points, personalized for the lead.
            4. No extra text, no explanations, no additional formatting—output must be pure JSON.

            Failure to strictly follow this format will result in incorrect output.
            """
          .formatted(leadDetails, context);
    }
  }
}
