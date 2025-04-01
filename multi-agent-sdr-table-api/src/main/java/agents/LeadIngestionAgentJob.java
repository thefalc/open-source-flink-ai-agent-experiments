/**
 * LeadIngestionAgentJob is a Flink streaming job that performs research on incoming leads
 * to enrich them with contextual insights for downstream scoring and engagement workflows.
 *
 * <p>Key Responsibilities:
 * <ul>
 *   <li>Consumes raw lead submissions from the "incoming_leads" Kafka topic.</li>
 *   <li>Enriches each lead using GPT-4 Turbo, guided by a structured research prompt.</li>
 *   <li>Incorporates auxiliary data via tools including Salesforce, Clearbit, and the lead's company website.</li>
 *   <li>Produces a research summary and lead context JSON to the "lead_ingestion_output" Kafka topic.</li>
 * </ul>
 *
 * <p>This job acts as the first step in the AI-driven sales funnel, automating research and segmentation
 * to support lead scoring, prioritization, and personalized outreach.
 */

package agents;

import org.apache.flink.table.functions.ScalarFunction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.stream.Collectors;

import org.apache.flink.table.api.TableEnvironment;

import java.util.List;

import io.github.cdimascio.dotenv.Dotenv;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.ChatModel;
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionTool;
import com.openai.models.chat.completions.ChatCompletionToolMessageParam;
import com.openai.models.chat.completions.ChatCompletionMessageToolCall;
import com.openai.models.chat.completions.ChatCompletionCreateParams;

import com.openai.models.FunctionDefinition;
import com.openai.models.FunctionParameters;
import com.openai.core.JsonValue;

import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.*;

import tools.AgentTools;
import util.Constants;
import util.KafkaConfig;

import java.util.Collection;
import java.util.Map;

public class LeadIngestionAgentJob {
    public static final String SYSTEM_PROMPT = "You're an Industry Research Specialist at StratusDB, a cloud-native, AI-powered data warehouse built for B2B "
            + "enterprises that need fast, scalable, and intelligent data infrastructure. StratusDB simplifies complex data "
            + "pipelines, enabling companies to store, query, and operationalize their data in real time.\n\n"
            + "Your role is to conduct research on potential leads to assess their fit for StratusAI Warehouse and provide key "
            + "insights for scoring and outreach planning. Your research will focus on industry trends, company background, "
            + "and AI adoption potential to ensure a tailored and strategic approach.";

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
                "CREATE TABLE `incoming_leads` (\n" +
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
                "CREATE TABLE `lead_ingestion_output` (\n" +
                        "raw_data STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'lead_ingestion_output',\n" +
                        "  'properties.bootstrap.servers' = '" + bootstrapServers + "',\n" +
                        "  'properties.security.protocol' = 'SASL_SSL',\n" +
                        "  'properties.sasl.mechanism' = 'PLAIN',\n" +
                        "  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'value.format' = 'raw'\n" +
                        ")"
        );

        // Process all incoming leads and write the agent output to the outgoing topic
        tEnv.createTemporarySystemFunction("LeadResearchFunction", new LeadResearchFunction(apiKey));
        tEnv.from("incoming_leads").select(call("LeadResearchFunction", $("raw_data")))
                .insertInto("lead_ingestion_output").execute();
    }

    public static class LeadResearchFunction extends ScalarFunction {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        private final String apiKey;

        public LeadResearchFunction(String apiKey) {
            this.apiKey = apiKey;
        }

        public String eval(String leadJson) {
            try {
                String cleanedJson = AgentTools.cleanJsonString(leadJson);

                JsonNode rootNode = objectMapper.readTree(cleanedJson);
                if (!rootNode.has("lead_data")) {
                    return leadJson; // If lead_data field is missing, return original JSON
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
                        .addDeveloperMessage(SYSTEM_PROMPT)
                        .addUserMessage(buildPrompt(leadJson, Constants.PRODUCT_DESCRIPTION));

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
                createParamsBuilder.addUserMessage(buildPrompt(leadJson, Constants.PRODUCT_DESCRIPTION));
                ChatCompletion chatCompletion = client.chat().completions().create(createParamsBuilder.build());
                String responseContent = chatCompletion.choices().stream()
                        .map(choice -> choice.message().content().orElse(""))
                        .collect(Collectors.joining("\n"));

                ((ObjectNode) rootNode).put("context", responseContent);

                System.out.println("Full response:\n" + responseContent);

                System.out.println("Root:");
                System.out.println(rootNode);

                return objectMapper.writeValueAsString(rootNode);
            } catch (Exception e) {
                e.printStackTrace();
                return "failed";
            }
        }

        private static String buildPrompt(String leadDetails, String productDescription) {
            return String.format("Using the lead input data, conduct preliminary research on the lead. Focus on finding relevant data\n" +
                    "that can aid in scoring the lead and planning a strategy to pitch them. You do not need to score the lead.\n" +
                    "\n" +
                    "Key Responsibilities:\n" +
                    "  - Analyze the lead's industry to identify relevant trends, market challenges, and AI adoption patterns.\n" +
                    "  - Gather company-specific insights, including size, market position, recent news, and strategic initiatives.\n" +
                    "  - Determine potential use cases for StratusAI Warehouse, focusing on how the company could benefit from real-time analytics, multi-cloud data management, and AI-driven optimization.\n" +
                    "  - Assess lead quality based on data completeness and engagement signals. Leads with short or vague form responses should be flagged for review but not immediately discarded.\n" +
                    "  - Use dedicated tools to enhance research and minimize manual work:\n" +
                    "    - Salesforce Data Access - Retrieves CRM data about the lead's past interactions, status, and engagement history.\n" +
                    "    - Company Website Lookup Tool - Fetches key details from the company's official website.\n" +
                    "    - Clearbit Enrichment API - Provides firmographic and contact-level data, including company size, funding, tech stack, and key decision-makers.\n" +
                    "  - Filter out weak leads or where the lead data doesn't look like a fit, ensuring minimal time is spent on companies unlikely to be a fit for StratusDB's offering.\n" +
                    "\n" +
                    "Lead Form Responses:\n" +
                    "  %s\n" +
                    "\n" +
                    "%s\n" +
                    "\n" +
                    "Expected Output - Research Report:\n" +
                    "The research report should be concise and actionable, containing:\n" +
                    "\n" +
                    "Industry Overview - Key trends, challenges, and AI adoption patterns in the lead's industry.\n" +
                    "Company Insights - Size, market position, strategic direction, and recent news.\n" +
                    "Potential Use Cases - How StratusAI Warehouse could provide value to the lead's company.\n" +
                    "Lead Quality Assessment - Based on available data, engagement signals, and fit for StratusDB's ideal customer profile.\n" +
                    "Additional Insights - Any relevant information that can aid in outreach planning or lead prioritization.", leadDetails, productDescription);
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
