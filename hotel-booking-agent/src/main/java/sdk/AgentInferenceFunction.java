package sdk;

import com.fasterxml.jackson.core.type.TypeReference;
import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.core.JsonValue;
import com.openai.models.ChatModel;
import com.openai.models.FunctionDefinition;
import com.openai.models.FunctionParameters;
import com.openai.models.chat.completions.*;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import com.fasterxml.jackson.databind.ObjectMapper;
import sdk.tools.Tool;
import sdk.tools.ToolRegistry;
import tools.AgentTools;

import java.util.*;
import java.util.stream.Collectors;

public class AgentInferenceFunction extends ScalarFunction {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final String apiKey;
    private final String agentName;
    private static boolean initialized = false;

    public AgentInferenceFunction(String apiKey, String agentName) {
        this.apiKey = apiKey;
        this.agentName = agentName;
    }

    @Override
    public void open(FunctionContext context) {
        OpenAIClient client = OpenAIOkHttpClient.builder()
                .apiKey(this.apiKey)
                .build();

        ModelPredict.init(client);
        ModelToolsPrediction.init(client);

        if (!initialized) {
            AgentTools.bootstrapAgents("agents");
            initialized = true;
        }
    }

    public String eval(String context) {
        try {
            String cleanedJson = AgentTools.cleanJsonString(context);

            Agent agent = AgentRegistry.getRegisteredAgents()
                    .stream()
                    .filter(a -> a.getName().equals(agentName))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Agent not found: " + agentName));

            System.out.println("Executing agent: " + agent.getName());

            List<Tool> tools = ToolRegistry.getToolsForAgent(agent.getName());
            String response;
            if (!tools.isEmpty()) {
                response = ModelToolsPrediction.llm_tool_invoke(agent.getModel(), agent.getName(),
                        agent.getPrompt(cleanedJson), tools);
                System.out.println("currentContext : " + response);
            }
            else {
                response = ModelPredict.ml_predict(agent.getModel(), agent.getPrompt(cleanedJson));
            }

            System.out.println("Inference response: ");
            System.out.println(response);

            return response;
        } catch (Exception e) {
            e.printStackTrace();
            return e.getMessage();
        }
    }
}

// Simulated Flink ML_PREDICT function wrapper
class ModelPredict {
    private static OpenAIClient client;

    public static void init(OpenAIClient clientInstance) {
        client = clientInstance;
    }

    public static String ml_predict(ModelDefinition model, String input) {
        System.out.println("Input: ");
        System.out.println(input);

        System.out.println("System: ");
        System.out.println(model.getSystemPrompt());

        ChatCompletionCreateParams.Builder createParamsBuilder = ChatCompletionCreateParams.builder()
                .model(ChatModel.of(model.getProvider()))
                .maxCompletionTokens(2048)
                .addDeveloperMessage(model.getSystemPrompt())
                .addUserMessage(input);

        ChatCompletion chatCompletion = client.chat().completions().create(createParamsBuilder.build());
        String responseContent = chatCompletion.choices().stream()
                .map(choice -> choice.message().content().orElse(""))
                .collect(Collectors.joining("\n"));

        return responseContent;
    }
}

// Simulated Flink LLM_TOOL_INVOKE function wrapper
class ModelToolsPrediction {
    private static OpenAIClient client;

    public static void init(OpenAIClient clientInstance) {
        client = clientInstance;
    }

    public static String llm_tool_invoke(ModelDefinition model, String agentName, String input, List<Tool> tools) {
        ChatCompletionCreateParams.Builder builder = buildChatRequest(model, input, tools);

        // Initial model call
        ChatCompletion firstCall = client.chat().completions().create(builder.build());

        // Extract tool calls and update builder with their results
        processToolCalls(firstCall, agentName, builder);

        // Follow-up model call with tool results
        ChatCompletion secondCall = client.chat().completions().create(builder.build());

        return extractFinalResponse(secondCall);
    }

    private static ChatCompletionCreateParams.Builder buildChatRequest(ModelDefinition model, String input, List<Tool> tools) {
        ChatCompletionCreateParams.Builder builder = ChatCompletionCreateParams.builder()
                .model(ChatModel.of(model.getProvider()))
                .maxCompletionTokens(2048)
                .addDeveloperMessage(model.getSystemPrompt())
                .addUserMessage(input);

        for (Tool tool : tools) {
            Map<String, Object> properties = tool.getParameters().entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> Map.of("type", e.getValue())
                    ));

            builder.addTool(ChatCompletionTool.builder()
                    .function(FunctionDefinition.builder()
                            .name(tool.getName())
                            .description(tool.getDescription())
                            .parameters(FunctionParameters.builder()
                                    .putAdditionalProperty("type", JsonValue.from("object"))
                                    .putAdditionalProperty("properties", JsonValue.from(properties))
                                    .putAdditionalProperty("required", JsonValue.from(new ArrayList<>(tool.getParameters().keySet())))
                                    .putAdditionalProperty("additionalProperties", JsonValue.from(false))
                                    .build())
                            .build())
                    .build());
        }

        return builder;
    }

    private static void processToolCalls(ChatCompletion response, String agentName, ChatCompletionCreateParams.Builder builder) {
        ObjectMapper objectMapper = new ObjectMapper();

        response.choices().stream()
                .map(ChatCompletion.Choice::message)
                .peek(builder::addMessage)
                .flatMap(message -> message.toolCalls().stream().flatMap(Collection::stream))
                .forEach(toolCall -> {
                    String toolName = toolCall.function().name();

                    try {
                        String rawJson = toolCall.function().arguments();
                        Map<String, Object> args = objectMapper.readValue(rawJson, new TypeReference<>() {});

                        Tool tool = ToolRegistry.getToolForAgent(agentName, toolName)
                                .orElseThrow(() -> new RuntimeException("Tool not found: " + toolName));

                        String toolResult = tool.invoke(args);

                        builder.addMessage(ChatCompletionToolMessageParam.builder()
                                .toolCallId(toolCall.id())
                                .content(toolResult)
                                .build());

                    } catch (Exception e) {
                        throw new RuntimeException("Failed to invoke tool: " + toolName, e);
                    }
                });
    }

    private static String extractFinalResponse(ChatCompletion response) {
        return response.choices().stream()
                .map(choice -> choice.message().content().orElse(""))
                .collect(Collectors.joining("\n"));
    }
}