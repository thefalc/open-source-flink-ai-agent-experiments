/**
 * NurtureCampaignAgentJob is a Flink streaming job that generates personalized, multi-touch nurture email
 * campaigns for leads marked as "Nurture" by the scoring pipeline.
 *
 * <p>Key Responsibilities:
 * <ul>
 *   <li>Consumes scored leads from the "lead_scoring_output" Kafka topic.</li>
 *   <li>Filters for leads with a next_step of "Nurture".</li>
 *   <li>Asynchronously generates a 3-email nurture sequence using GPT-4 Turbo, informed by lead data and enrichment tools.</li>
 *   <li>Utilizes tools like Clearbit, Salesforce, LinkedIn, and company websites to personalize campaign content.</li>
 *   <li>Outputs structured JSON campaigns to the "email_campaigns" Kafka topic for downstream orchestration.</li>
 * </ul>
 *
 * <p>This job helps build engagement over time by delivering context-aware, content-driven email sequences
 * to prospects who are not yet ready for direct sales outreach.
 */
package agents;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.core.JsonValue;
import com.openai.models.ChatModel;
import com.openai.models.FunctionDefinition;
import com.openai.models.FunctionParameters;
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.chat.completions.ChatCompletionTool;
import com.openai.models.chat.completions.ChatCompletionMessageToolCall;
import com.openai.models.chat.completions.ChatCompletionToolMessageParam;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import tools.AgentTools;
import util.KafkaConfig;
import util.Constants;

import java.util.Collection;
import java.util.Map;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class NurtureCampaignAgentJob {
    public static final String SYSTEM_PROMPT =  "You're the AI Nurture Campaign Specialist at StratusDB, a cloud-native, AI-powered data\n" +
            "warehouse built for B2B enterprises that need fast, scalable, and intelligent data\n" +
            "infrastructure. StratusDB simplifies complex data pipelines, enabling companies to store,\n" +
            "query, and operationalize their data in real time.\n" +
            "\n" +
            "You design multi-step nurture campaigns that educate prospects and drive engagement over time.\n" +
            "Your emails are personalized, strategically sequenced, and content-driven, ensuring relevance at every stage.";

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

        // Define the topic to write the agent output to
        tEnv.executeSql(
                "CREATE TABLE `email_campaigns` (\n" +
                        "raw_data STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'email_campaigns',\n" +
                        "  'properties.bootstrap.servers' = '" + bootstrapServers + "',\n" +
                        "  'properties.security.protocol' = 'SASL_SSL',\n" +
                        "  'properties.sasl.mechanism' = 'PLAIN',\n" +
                        "  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'value.format' = 'raw'\n" +
                        ")"
        );

        // Process all incoming leads and write the agent output to the outgoing topic
        tEnv.createTemporarySystemFunction("NurtureCampaignFunction", new NurtureCampaignFunction(apiKey));
        tEnv.from("lead_scoring_output").select(call("NurtureCampaignFunction", $("raw_data")).as("campaign_email"))
                .filter($("campaign_email").isNotEqual(""))
                .insertInto("email_campaigns").execute();
    }

    public static class NurtureCampaignFunction extends ScalarFunction {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        private final String apiKey;

        public NurtureCampaignFunction(String apiKey) {
            this.apiKey = apiKey;
        }

        public String eval(String leadJson) {
            try {
                String cleanedJson = AgentTools.cleanJsonString(leadJson);

                JsonNode rootNode = objectMapper.readTree(cleanedJson);
                if (!rootNode.has("lead_data")) {
                    return leadJson; // If lead_data field is missing, return original JSON
                }

                // Add score to JSON
                ObjectNode leadDataNode = (ObjectNode) rootNode.get("lead_data");
                ObjectNode leadEvaluationNode = (ObjectNode) rootNode.get("lead_evaluation");

                if (!leadEvaluationNode.get("next_step").asText().equals("Nurture")) {
                    return "";
                }

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
                return e.getMessage().toString();
            }
        }

        private static String buildPrompt(String leadDetails, String context, String productDescription) {
            return String.format("Using the lead input and evaluation data, craft a 3-email nurture campaign designed to warm up the\n" +
                    "prospect and gradually build engagement over time. Each email should be sequenced strategically,\n" +
                    "introducing relevant insights, addressing pain points, and progressively guiding the lead toward a conversation.\n" +
                    "Link to additional marketing assets when it makes sense.\n" +
                    "\n" +
                    "Key Responsibilities:\n" +
                    "- Personalize each email based on lead insights from Company Website, LinkedIn, Salesforce, and Clearbit.\n" +
                    "- Structure a 3-email sequence, ensuring each email builds upon the previous one and provides increasing value.\n" +
                    "- Align messaging with the prospect's industry, role, and pain points, demonstrating how StratusAI Warehouse can address their challenges.\n" +
                    "- Link to relevant content assets (case studies, blog posts, whitepapers, webinars, etc.) by leveraging a Content Search Tool to find the most valuable follow-up materials.\n" +
                    "\n" +
                    "Tools & Data Sources:\n" +
                    "- Company Website Lookup Tool - Extracts company details, news, and strategic initiatives.\n" +
                    "- Salesforce Data Access - Retrieves CRM insights on past interactions, engagement status, and previous outreach.\n" +
                    "- Clearbit Enrichment API - Provides firmographic and contact-level data, including company size, funding, tech stack, and key decision-makers.\n" +
                    "- LinkedIn Profile API - Gathers professional history, recent activity, and mutual connections for better personalization.\n" +
                    "\n" +
                    "Lead Data:\n" +
                    "- Lead Form Responses: %s\n" +
                    "- Lead Evaluation: %s\n" +
                    "\n" +
                    "%s\n" +
                    "\n" +
                    "Expected Output - 3-Email Nurture Campaign:\n" +
                    "Each email should be concise, engaging, and sequenced effectively, containing:\n" +
                    "1. Personalized Opening - Address the lead by name and reference a relevant insight from their company, role, or industry trends.\n" +
                    "2. Key Challenge & Value Proposition - Identify a pain point or opportunity based on lead data and explain how StratusAI Warehouse solves it.\n" +
                    "3. Relevant Content Asset - Include a blog post, case study, or whitepaper that aligns with the lead's interests.\n" +
                    "4. Clear Call to Action (CTA) - Encourage engagement with a low-friction action (e.g., reading content, replying, scheduling a chat).\n" +
                    "5. Progressive Value Addition - Ensure each email builds upon the last, gradually increasing lead engagement and urgency.\n" +
                    "\n" +
                    "Output Format\n" +
                    "- The output must be strictly formatted as JSON, with no additional text, commentary, or explanation.\n" +
                    "- Make sure the JSON format is valid. If not, regenerate with valid JSON.\n" +
                    "- The JSON must strictly follow this structure:\n" +
                    "{\n" +
                    "  \"emails\": [\n" +
                    "    {\n" +
                    "      \"to\": \"[Lead's Email Address]\",\n" +
                    "      \"subject\": \"[Subject Line for Email 1]\",\n" +
                    "      \"body\": \"[Email Body for Email 1]\"\n" +
                    "    },\n" +
                    "    {\n" +
                    "      \"to\": \"[Lead's Email Address]\",\n" +
                    "      \"subject\": \"[Subject Line for Email 2]\",\n" +
                    "      \"body\": \"[Email Body for Email 2]\"\n" +
                    "    },\n" +
                    "    {\n" +
                    "      \"to\": \"[Lead's Email Address]\",\n" +
                    "      \"subject\": \"[Subject Line for Email 3]\",\n" +
                    "      \"body\": \"[Email Body for Email 3]\"\n" +
                    "    }\n" +
                    "  ]\n" +
                    "}\n" +
                    "\n" +
                    "Failure to strictly follow this format will result in incorrect output.", leadDetails,
                    context, productDescription);
        }

        private static String callFunction(ChatCompletionMessageToolCall.Function function) {
            if (function.name().equals("getCompanyWebsite")) {
                return "";//AgentTools.getCompanyWebsite(function);
            } else if (function.name().equals("getSalesforceData")) {
                return "";//AgentTools.getSalesforceData(function);
            } else if (function.name().equals("getClearbitData")) {
                return "";//AgentTools.getClearbitData(function);
            } else if (function.name().equals("getRecentLinkedInPosts")) {
                return "";//AgentTools.getRecentLinkedInPosts(function);
            }

            throw new IllegalArgumentException("Unknown function: " + function.name());
        }
    }
}
