package sdk;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.ChatModel;
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import com.fasterxml.jackson.databind.ObjectMapper;
import sdk.tools.Tool;
import sdk.tools.ToolRegistry;
import tools.AgentTools;

import java.util.Arrays;
import java.util.List;
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
            String currentContext = cleanedJson;
            if (!tools.isEmpty()) {
                currentContext = ModelToolsPrediction.llm_tool_invoke(agent.getModel(), currentContext, tools);
            }

//            String response = ModelPredict.ml_predict(agent.getModel(), agent.getPrompt(cleanedJson));


            System.out.println("Inference response: ");
            System.out.println(currentContext);

            return currentContext;
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

    public static String llm_tool_invoke(ModelDefinition model, String input, List<Tool> tools) {
        System.out.println("Input: ");
        System.out.println(input);

        System.out.println("System: ");
        System.out.println(model.getSystemPrompt());

        for (Tool tool : tools) {
            System.out.println("Tool: " + tool.getName());
        }

        return "";

//        ChatCompletionCreateParams.Builder createParamsBuilder = ChatCompletionCreateParams.builder()
//                .model(ChatModel.of(model.getProvider()))
//                .maxCompletionTokens(2048)
//                .addDeveloperMessage(model.getSystemPrompt())
//                .addUserMessage(input);
//
//        ChatCompletion chatCompletion = client.chat().completions().create(createParamsBuilder.build());
//        String responseContent = chatCompletion.choices().stream()
//                .map(choice -> choice.message().content().orElse(""))
//                .collect(Collectors.joining("\n"));
//
//        return responseContent;
    }
}