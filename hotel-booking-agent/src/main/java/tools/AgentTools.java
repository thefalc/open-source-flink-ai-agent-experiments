package tools;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.ChatModel;
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionTool;
import com.openai.models.chat.completions.ChatCompletionToolMessageParam;
import com.openai.models.chat.completions.ChatCompletionMessageToolCall;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.chat.completions.ChatCompletionMessageToolCall;
import com.openai.models.FunctionDefinition;
import com.openai.models.FunctionParameters;
import com.openai.core.JsonObject;
import com.openai.core.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;

import static com.openai.core.ObjectMappers.jsonMapper;

import io.github.cdimascio.dotenv.Dotenv;

import java.security.SecureRandom;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;
import util.Constants;

public class AgentTools {
    // Pattern to remove non-printable ASCII and non-UTF characters
    private static final Pattern INVALID_CHARS = Pattern.compile("[^\\x20-\\x7E]");

    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final SecureRandom random = new SecureRandom();

    private static String getApiKey() {
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

        return dotenv.get("OPENAI_API_KEY");
    }

  public static String generateRandomString(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(CHARACTERS.charAt(random.nextInt(CHARACTERS.length())));
    }
    return sb.toString();
  }

    public static void bootstrapAgents(String basePackage) {
        try (ScanResult scanResult = new ClassGraph()
                .enableClassInfo()
                .acceptPackages(basePackage)
                .scan()) {
            scanResult.getClassesImplementing("sdk.Agent")
                    .loadClasses()
                    .forEach(clazz -> {
                        try {
                            Class.forName(clazz.getName());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
        }
    }

    /**
     * Cleans the input JSON string by removing non-printable and invalid
     * characters.
     */
    public static String cleanJsonString(String json) {
        // Removes characters outside printable ASCII range
        return INVALID_CHARS.matcher(json).replaceAll("");
    }

    public static String getRecentLinkedInPosts(ChatCompletionMessageToolCall.Function function) {
        System.out.println("getRecentLinkedInPosts");
        JsonValue arguments;
        try {
            arguments = JsonValue.from(jsonMapper().readTree(function.arguments()));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Bad function arguments", e);
        }

        System.out.println(arguments);

        OpenAIClient client = OpenAIOkHttpClient.builder()
                .apiKey(getApiKey())
                .build();

        String prompt = String.format("Using the lead details, create some fake data that represents what the "
                + "lead has recently been talking about on LinkedIn. Keep this short. This "
                + "is to inform the email campaign to the lead.\n\n"
                + "Lead details: %s", arguments);

        System.out.println(prompt);

        ChatCompletionCreateParams.Builder createParamsBuilder = ChatCompletionCreateParams.builder()
                .model(ChatModel.GPT_4_TURBO)
                .maxCompletionTokens(2048)
                .addUserMessage(prompt);

        ChatCompletion chatCompletion = client.chat().completions().create(createParamsBuilder.build());
        String responseContent = chatCompletion.choices().stream()
                .map(choice -> choice.message().content().orElse(""))
                .collect(Collectors.joining("\n"));

        return responseContent;
    }

    public static String getClearbitData(ChatCompletionMessageToolCall.Function function) {
        System.out.println("getClearbitData");
        JsonValue arguments;
        try {
            arguments = JsonValue.from(jsonMapper().readTree(function.arguments()));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Bad function arguments", e);
        }

        System.out.println(arguments);

        OpenAIClient client = OpenAIOkHttpClient.builder()
                .apiKey(getApiKey())
                .build();

        String enrichedData = "{\n" +
                "    \"person\": {\n" +
                "        \"full_name\": \"Jane Doe\",\n" +
                "        \"job_title\": \"Director of Data Engineering\",\n" +
                "        \"company_name\": \"Acme Analytics\",\n" +
                "        \"company_domain\": \"acmeanalytics.com\",\n" +
                "        \"work_email\": \"jane.doe@acmeanalytics.com\",\n" +
                "        \"linkedin_url\": \"https://www.linkedin.com/in/janedoe\",\n" +
                "        \"twitter_handle\": \"@janedoe\",\n" +
                "        \"location\": {\n" +
                "            \"city\": \"San Francisco\",\n" +
                "            \"state\": \"California\",\n" +
                "            \"country\": \"United States\"\n" +
                "        },\n" +
                "        \"work_phone\": \"+1 415-555-1234\",\n" +
                "        \"employment_history\": [\n" +
                "            {\n" +
                "                \"company\": \"DataCorp\",\n" +
                "                \"job_title\": \"Senior Data Engineer\",\n" +
                "                \"years\": \"2018-2022\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"company\": \"Tech Solutions\",\n" +
                "                \"job_title\": \"Data Analyst\",\n" +
                "                \"years\": \"2015-2018\"\n" +
                "            }\n" +
                "        ]\n" +
                "    },\n" +
                "    \"company\": {\n" +
                "        \"name\": \"Acme Analytics\",\n" +
                "        \"domain\": \"acmeanalytics.com\",\n" +
                "        \"industry\": \"Data & Analytics\",\n" +
                "        \"sector\": \"Software & IT Services\",\n" +
                "        \"employee_count\": 500,\n" +
                "        \"annual_revenue\": \"$50M-$100M\",\n" +
                "        \"company_type\": \"Private\",\n" +
                "        \"headquarters\": {\n" +
                "            \"city\": \"San Francisco\",\n" +
                "            \"state\": \"California\",\n" +
                "            \"country\": \"United States\"\n" +
                "        },\n" +
                "        \"linkedin_url\": \"https://www.linkedin.com/company/acme-analytics\",\n" +
                "        \"twitter_handle\": \"@acmeanalytics\",\n" +
                "        \"facebook_url\": \"https://www.facebook.com/acmeanalytics\",\n" +
                "        \"technologies_used\": [\n" +
                "            \"AWS\",\n" +
                "            \"Snowflake\",\n" +
                "            \"Apache Kafka\",\n" +
                "            \"Flink\",\n" +
                "            \"Looker\",\n" +
                "            \"Salesforce\"\n" +
                "        ],\n" +
                "        \"funding_info\": {\n" +
                "            \"total_funding\": \"$75M\",\n" +
                "            \"last_round\": \"Series B\",\n" +
                "            \"last_round_date\": \"2023-08-15\",\n" +
                "            \"investors\": [\n" +
                "                \"Sequoia Capital\",\n" +
                "                \"Andreessen Horowitz\"\n" +
                "            ]\n" +
                "        },\n" +
                "        \"key_decision_makers\": [\n" +
                "            {\n" +
                "                \"name\": \"John Smith\",\n" +
                "                \"title\": \"CEO\",\n" +
                "                \"linkedin_url\": \"https://www.linkedin.com/in/johnsmith\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"name\": \"Emily Johnson\",\n" +
                "                \"title\": \"VP of Engineering\",\n" +
                "                \"linkedin_url\": \"https://www.linkedin.com/in/emilyjohnson\"\n" +
                "            }\n" +
                "        ],\n" +
                "        \"hiring_trends\": {\n" +
                "            \"open_positions\": 12,\n" +
                "            \"growth_rate\": \"15% YoY\",\n" +
                "            \"top_hiring_departments\": [\n" +
                "                \"Engineering\",\n" +
                "                \"Data Science\",\n" +
                "                \"Sales\"\n" +
                "            ]\n" +
                "        }\n" +
                "    }\n" +
                "}";

        String prompt = String.format("Take the lead details and generate realistic Clearbit data to represent the enriched lead.\n" +
                "Return only the fake Clearbit data as JSON. Do not wrap the message in any additional text.\n\n" +
                "Lead details:\n" +
                "  %s\n\n" +
                "The fake output should look like this:\n" +
                "  %s\n", arguments, enrichedData);

        System.out.println(prompt);

        ChatCompletionCreateParams.Builder createParamsBuilder = ChatCompletionCreateParams.builder()
                .model(ChatModel.GPT_4_TURBO)
                .maxCompletionTokens(2048)
                .addUserMessage(prompt);

        ChatCompletion chatCompletion = client.chat().completions().create(createParamsBuilder.build());
        String responseContent = chatCompletion.choices().stream()
                .map(choice -> choice.message().content().orElse(""))
                .collect(Collectors.joining("\n"));

        return responseContent;
    }

    public static String getSalesforceData(ChatCompletionMessageToolCall.Function function) {
        System.out.println("getSalesforceData");
        JsonValue arguments;
        try {
            arguments = JsonValue.from(jsonMapper().readTree(function.arguments()));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Bad function arguments", e);
        }

        System.out.println("arguments:");
        System.out.println(arguments);
        System.out.println("apiKey: " + getApiKey());

        OpenAIClient client = OpenAIOkHttpClient.builder()
                .apiKey(getApiKey())
                .build();

        String prompt = String.format("Take the lead details and generate realistic Salesforce data to represent the contact,\n" +
                "company, lead information, and any historical interactions we've had with the lead.\n\n" +
                "Take into account the product details when generating the history. If there's not a good\n" +
                "match between the lead and product, reflect that in the Salesforce data.\n\n" +
                "It's also ok to return no information to simulate that there's no history with this lead.\n\n" +
                "Return only the fake Salesforce data as JSON. Do not wrap the message in any additional text.\n\n" +
                "Lead details:\n" +
                "  %s\n\n" +
                "Product details:\n" +
                "  %s\n", arguments, Constants.PRODUCT_DESCRIPTION);

        System.out.println(prompt);

        ChatCompletionCreateParams.Builder createParamsBuilder = ChatCompletionCreateParams.builder()
                .model(ChatModel.GPT_4_TURBO)
                .addUserMessage(prompt);

        ChatCompletion chatCompletion = client.chat().completions().create(createParamsBuilder.build());
        String responseContent = chatCompletion.choices().stream()
                .map(choice -> choice.message().content().orElse(""))
                .collect(Collectors.joining("\n"));

        return responseContent;
    }

    public static String getCompanyWebsite(ChatCompletionMessageToolCall.Function function) {
        System.out.println("getCompanyWebsite");
        JsonValue arguments;
        try {
            arguments = JsonValue.from(jsonMapper().readTree(function.arguments()));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Bad function arguments", e);
        }

        String url = ((JsonObject) arguments).values().get("url").asStringOrThrow();

        System.out.println("Url: " + url);

        OpenAIClient client = OpenAIOkHttpClient.builder()
                .apiKey(getApiKey())
                .build();

        String prompt = String.format("Take the URL and generate realistic homepage data for this company.\n\n" +
                "Return only the fake homepage data. Do not wrap the message in any additional text.\n\n" +
                "URL:\n" +
                "  %s\n", url);

        System.out.println(prompt);

        ChatCompletionCreateParams.Builder createParamsBuilder = ChatCompletionCreateParams.builder()
                .model(ChatModel.GPT_4_TURBO)
                .maxCompletionTokens(2048)
                .addUserMessage(prompt);

        ChatCompletion chatCompletion = client.chat().completions().create(createParamsBuilder.build());
        String responseContent = chatCompletion.choices().stream()
                .map(choice -> choice.message().content().orElse(""))
                .collect(Collectors.joining("\n"));

        return responseContent;
    }
}
