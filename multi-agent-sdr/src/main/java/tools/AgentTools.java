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

import java.util.regex.Pattern;
import java.util.stream.Collectors;

import util.Constants;

public class AgentTools {
  // Pattern to remove non-printable ASCII and non-UTF characters
  private static final Pattern INVALID_CHARS = Pattern.compile("[^\\x20-\\x7E]");

  private static String getApiKey() {
    Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

    return dotenv.get("OPENAI_API_KEY");
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

    String prompt = """
        Using the lead details, create some fake data that represents what the
        lead has recently been talking about on LinkedIn. Keep this short. This
        is to inform the email campaign to the lead.

        Lead details: %s
        """.formatted(arguments);

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

    String enrichedData = """
        {
            "person": {
                "full_name": "Jane Doe",
                "job_title": "Director of Data Engineering",
                "company_name": "Acme Analytics",
                "company_domain": "acmeanalytics.com",
                "work_email": "jane.doe@acmeanalytics.com",
                "linkedin_url": "https://www.linkedin.com/in/janedoe",
                "twitter_handle": "@janedoe",
                "location": {
                    "city": "San Francisco",
                    "state": "California",
                    "country": "United States"
                },
                "work_phone": "+1 415-555-1234",
                "employment_history": [
                    {
                        "company": "DataCorp",
                        "job_title": "Senior Data Engineer",
                        "years": "2018-2022"
                    },
                    {
                        "company": "Tech Solutions",
                        "job_title": "Data Analyst",
                        "years": "2015-2018"
                    }
                ]
            },
            "company": {
                "name": "Acme Analytics",
                "domain": "acmeanalytics.com",
                "industry": "Data & Analytics",
                "sector": "Software & IT Services",
                "employee_count": 500,
                "annual_revenue": "$50M-$100M",
                "company_type": "Private",
                "headquarters": {
                    "city": "San Francisco",
                    "state": "California",
                    "country": "United States"
                },
                "linkedin_url": "https://www.linkedin.com/company/acme-analytics",
                "twitter_handle": "@acmeanalytics",
                "facebook_url": "https://www.facebook.com/acmeanalytics",
                "technologies_used": [
                    "AWS",
                    "Snowflake",
                    "Apache Kafka",
                    "Flink",
                    "Looker",
                    "Salesforce"
                ],
                "funding_info": {
                    "total_funding": "$75M",
                    "last_round": "Series B",
                    "last_round_date": "2023-08-15",
                    "investors": [
                        "Sequoia Capital",
                        "Andreessen Horowitz"
                    ]
                },
                "key_decision_makers": [
                    {
                        "name": "John Smith",
                        "title": "CEO",
                        "linkedin_url": "https://www.linkedin.com/in/johnsmith"
                    },
                    {
                        "name": "Emily Johnson",
                        "title": "VP of Engineering",
                        "linkedin_url": "https://www.linkedin.com/in/emilyjohnson"
                    }
                ],
                "hiring_trends": {
                    "open_positions": 12,
                    "growth_rate": "15% YoY",
                    "top_hiring_departments": [
                        "Engineering",
                        "Data Science",
                        "Sales"
                    ]
                }
            }
        }
        """;

    String prompt = """
        Take the lead details and generate realistic Clearbit data to represent the enriched lead.
        Return only the fake Clearbit data as JSON. Do not wrap the message in any additional text.

        Lead details:
          %s

        The fake output should look like this:
          %s
        """.formatted(arguments, enrichedData);

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

    System.out.println(arguments);

    OpenAIClient client = OpenAIOkHttpClient.builder()
        .apiKey(getApiKey())
        .build();

    String prompt = """
        Take the lead details and generate realistic Salesforce data to represent the contact,
        company, lead information, and any historical interactions we've had with the lead.

        Take into account the product details when generating the history. If there's not a good
        match between the lead and product, reflect that in the Salesforce data.

        It's also ok to return no information to simulate that there's no history with this lead.

        Return only the fake Salesforce data as JSON. Do not wrap the message in any additional text.

        Lead details:
          %s

        Product details:
          %s
        """.formatted(arguments, Constants.PRODUCT_DESCRIPTION);

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

    String prompt = """
        Take the URL and generate realistic homepage data for this company.

        Return only the fake homepage data. Do not wrap the message in any additional text.

       URL:
          %s
        """.formatted(url);

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
