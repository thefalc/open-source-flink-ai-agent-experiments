/**
 * ActiveOutreachAgentJob is a Flink streaming job that listens for high-quality leads from a Kafka topic,
 * filters those flagged for active engagement, and generates personalized outreach emails using OpenAI.
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

public class ActiveOutreachAgentJob {
    public static final String SYSTEM_PROMPT =  "You're the AI Email Engagement Specialist at StratusDB, a cloud-native, AI-powered data warehouse built for B2B\n" +
            "enterprises that need fast, scalable, and intelligent data infrastructure. StratusDB simplifies complex data\n" +
            "pipelines, enabling companies to store, query, and operationalize their data in real time.\n" +
            "\n" +
            "You craft engaging, high-converting emails that capture attention, drive conversations, and move leads forward.\n" +
            "Your messaging is personalized, data-driven, and aligned with industry pain points to ensure relevance and impact.\n" +
            "\n" +
            "Your role is to write compelling outreach emails, optimize engagement through A/B testing and behavioral insights,\n" +
            "and ensure messaging resonates with each prospect's needs and challenges.";

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
        tEnv.createTemporarySystemFunction("ActiveOutreachFunction", new ActiveOutreachFunction(apiKey));
        tEnv.from("lead_scoring_output").select(call("ActiveOutreachFunction", $("raw_data")).as("campaign_email"))
                .filter($("campaign_email").isNotEqual(""))
                .insertInto("email_campaigns").execute();
    }

    public static class ActiveOutreachFunction extends ScalarFunction {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        private final String apiKey;

        public ActiveOutreachFunction(String apiKey) {
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

                if (!leadEvaluationNode.get("next_step").asText().equals("Actively Engage")) {
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
            return String.format("Using the lead input and evaluation data to craft a highly personalized and engaging email to initiate a conversation with the prospect.\n" +
                            "The email should be tailored to their industry, role, and business needs, ensuring relevance and increasing the likelihood of a response.\n" +
                            "\n" +
                            "Key Responsibilities:\n" +
                            "- Personalize outreach based on lead insights from company website, LinkedIn, Salesforce, and Clearbit.\n" +
                            "- Craft a compelling email structure, ensuring clarity, relevance, and engagement.\n" +
                            "- Align messaging with the prospect's pain points and industry trends, showing how StratusAI Warehouse addresses their challenges.\n" +
                            "\n" +
                            "Use dedicated tools to enhance personalization and optimize engagement:\n" +
                            "- Company Website Lookup Tool - Extracts relevant company details, recent news, and strategic initiatives.\n" +
                            "- Salesforce Data Access - Retrieves CRM data about the lead's past interactions, engagement status, and any prior outreach.\n" +
                            "- Clearbit Enrichment API - Provides firmographic and contact-level data, including company size, funding, tech stack, and key decision-makers.\n" +
                            "- LinkedIn Profile API - Gathers recent activity and mutual connections to inform messaging.\n" +
                            "\n" +
                            "Ensure a clear and actionable CTA, encouraging the lead to engage without high friction.\n" +
                            "\n" +
                            "Lead Data\n" +
                            "- Lead Form Responses: %s\n" +
                            "- Lead Evaluation: %s\n" +
                            "\n" +
                            "%s\n" +
                            "\n" +
                            "Expected Output - Personalized Prospect Email:\n" +
                            "The email should be concise, engaging, and structured to drive a response, containing:\n" +
                            "\n" +
                            "- Personalized Opening - Address the lead by name and reference a relevant insight from their company, role, or industry trends.\n" +
                            "- Key Challenge & Value Proposition - Identify a pain point or opportunity based on lead data and explain how StratusAI Warehouse solves it.\n" +
                            "- Clear Call to Action (CTA) - Encourage a response with a low-friction action, such as scheduling a quick chat or sharing feedback.\n" +
                            "- Engagement-Oriented Tone - Maintain a conversational yet professional approach, keeping the message brief and impactful.\n" +
                            "\n" +
                            "Output Format\n" +
                            "- The output must be strictly formatted as JSON, with no additional text, commentary, or explanation.\n" +
                            "- The JSON should exactly match the following structure:\n" +
                            "  {\n" +
                            "      \"to\": \"Lead's Email Address\",\n" +
                            "      \"subject\": \"Example Subject Line\",\n" +
                            "      \"body\": \"Example Email Body\"\n" +
                            "  }\n" +
                            "\n" +
                            "Failure to strictly follow this format will result in incorrect output.\n"
                    , leadDetails, context, productDescription);
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
