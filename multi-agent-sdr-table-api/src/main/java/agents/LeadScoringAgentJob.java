/**
 * LeadScoringAgentJob is a Flink streaming job that processes enriched lead data and applies AI-driven logic
 * to generate lead scores, recommended next steps, and talking points for sales outreach.
 *
 * <p>Key Responsibilities:
 * <ul>
 *   <li>Consumes researched leads from the "lead_ingestion_output" Kafka topic.</li>
 *   <li>Asynchronously scores each lead using GPT-4 Turbo based on form responses, firmographic data, and research context.</li>
 *   <li>Evaluates lead fit based on buyer persona, company profile, and readiness signals.</li>
 *   <li>Outputs a structured evaluation (score, next step, and talking points) to the "lead_scoring_output" Kafka topic.</li>
 * </ul>
 */
package agents;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.ChatModel;
import com.openai.models.chat.completions.*;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import tools.AgentTools;
import util.KafkaConfig;

import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class LeadScoringAgentJob {
    public static final String SYSTEM_PROMPT =  "You're the Lead Scoring and Strategic Planner at StratusDB, a cloud-native, AI-powered data warehouse built for B2B\n" +
            "enterprises that need fast, scalable, and intelligent data infrastructure. StratusDB simplifies complex data\n" +
            "pipelines, enabling companies to store, query, and operationalize their data in real time.\n" +
            "\n" +
            "You combine insights from lead analysis and research to score leads accurately and align them with the\n" +
            "optimal offering. Your strategic vision and scoring expertise ensure that\n" +
            "potential leads are matched with solutions that meet their specific needs.\n" +
            "\n" +
            "You role is to utilize analyzed data and research findings to score leads, suggest next steps, and identify talking points.";

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
        String apiKey = dotenv.get("OPENAI_API_KEY");

        KafkaConfig kafkaConfig = new KafkaConfig("client.properties");
        String bootstrapServers = kafkaConfig.getBootstrapServers();
        String username = kafkaConfig.getUsername();
        String password = kafkaConfig.getPassword();

        // Define the input for the agent
        tEnv.executeSql(
                "CREATE TABLE `lead_ingestion_output` (\n" +
                        "raw_data STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'incoming_leads',\n" +
                        "  'properties.bootstrap.servers' = '" + bootstrapServers + "',\n" +
                        "  'properties.security.protocol' = 'SASL_SSL',\n" +
                        "  'properties.sasl.mechanism' = 'PLAIN',\n" +
                        "  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'value.format' = 'raw'\n" +
                        ")"
        );

        // Define the topic to write the agent output to
        tEnv.executeSql(
                "CREATE TABLE `lead_scoring_output` (\n" +
                        "raw_data STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'lead_scoring_output',\n" +
                        "  'properties.bootstrap.servers' = '" + bootstrapServers + "',\n" +
                        "  'properties.security.protocol' = 'SASL_SSL',\n" +
                        "  'properties.sasl.mechanism' = 'PLAIN',\n" +
                        "  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'value.format' = 'raw'\n" +
                        ")"
        );

        // Process all incoming leads and write the agent output to the outgoing topic
        tEnv.createTemporarySystemFunction("LeadScoringFunction", new LeadScoringFunction(apiKey));
        tEnv.from("lead_ingestion_output").select(call("LeadScoringFunction", $("raw_data"))).limit(1)
                .insertInto("lead_scoring_output").execute();
    }

    public static class LeadScoringFunction extends ScalarFunction {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        private final String apiKey;

        public LeadScoringFunction(String apiKey) {
            this.apiKey = apiKey;
        }

        public String eval(String leadJson) {
            try {
                String cleanedJson = AgentTools.cleanJsonString(leadJson);

                JsonNode rootNode = objectMapper.readTree(cleanedJson);
                if (!rootNode.has("lead_data")) {
                    return leadJson; // If lead_data field is missing, return original JSON
                }

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
                return e.getMessage().toString();
            }
        }

        private static String buildPrompt(String leadDetails, String context) {
            return String.format("Utilize the provided context and the lead's form response to score the lead.\n" +
                            "\n" +
                            "- Consider factors such as industry relevance, company size, StratusAI Warehouse use case potential, and buying readiness.\n" +
                            "- Evaluate the wording and length of the response—short answers are a yellow flag.\n" +
                            "- Take into account the role of the lead. Only prioritize leads that fit our core buyer persona. Nurture low quality.\n" +
                            "- Be pessimistic: focus high scores on leads with clear potential to close.\n" +
                            "- Smaller companies typically have lower budgets.\n" +
                            "- Avoid spending too much time on leads that are not a good fit.\n" +
                            "\n" +
                            "Lead Data\n" +
                            "- Lead Form Responses: %s\n" +
                            "- Additional Context: %s\n" +
                            "\n" +
                            "Output Format\n" +
                            "- The output must be strictly formatted as JSON, with no additional text, commentary, or explanation.\n" +
                            "- The JSON should exactly match the following structure:\n" +
                            "  {\n" +
                            "     \"score\": \"80\",\n" +
                            "     \"next_step\": \"Nurture | Actively Engage\",\n" +
                            "     \"talking_points\": \"Here are the talking points to engage the lead\"\n" +
                            "  }\n" +
                            "\n" +
                            "Formatting Rules\n" +
                            "  1. score: An integer between 0 and 100.\n" +
                            "  2. next_step: Either \"Nurture\" or \"Actively Engage\" (no variations).\n" +
                            "  3. talking_points: A list of at least three specific talking points, personalized for the lead.\n" +
                            "  4. No extra text, no explanations, no additional formatting—output must be pure JSON.\n" +
                            "\n" +
                            "Failure to strictly follow this format will result in incorrect output.\n"
            , leadDetails, context);
        }
    }
}