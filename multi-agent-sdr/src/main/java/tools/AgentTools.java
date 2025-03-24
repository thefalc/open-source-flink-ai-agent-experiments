package tools;

import com.openai.models.chat.completions.ChatCompletionMessageToolCall;
import com.openai.models.FunctionDefinition;
import com.openai.models.FunctionParameters;
import com.openai.core.JsonObject;
import com.openai.core.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import static com.openai.core.ObjectMappers.jsonMapper;

import java.util.regex.Pattern;

public class AgentTools {
  // Pattern to remove non-printable ASCII and non-UTF characters
  private static final Pattern INVALID_CHARS = Pattern.compile("[^\\x20-\\x7E]");

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

    return "LinkedIn Recent Activity:\\n" + //
                "- Liked a post about B2B sales productivity tools\\n" + //
                "- Commented on an article about restaurant technology trends\\n" + //
                "- Shared a post about improving sales team performance in the hospitality industry\\n" + //
                "\\n" + //
                "This fictional activity suggests potential interest areas that could be relevant for an email campaign targeting Sean, focusing on sales efficiency and restaurant/hospitality technology solutions.";
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

    // String email = ((JsonObject) arguments).values().get("email").asStringOrThrow();
    // String companyWebsite = ((JsonObject) arguments).values().get("company_website").asStringOrThrow();
    // String companyName = ((JsonObject) arguments).values().get("company_name").asStringOrThrow();
    // String title = ((JsonObject) arguments).values().get("job_title").asStringOrThrow();
    // String name = ((JsonObject) arguments).values().get("name").asStringOrThrow();

    // System.out.println(email + " " + companyWebsite);

    return "{\n" +
         "    \"person\": {\n" +
         "        \"full_name\": \"Sean Smith\",\n" +
         "        \"job_title\": \"Account Executive\",\n" +
         "        \"company_name\": \"Rich Table\",\n" +
         "        \"company_domain\": \"richtablesf.com\",\n" +
         "        \"work_email\": \"sean.smith@richtablesf.com\",\n" +
         "        \"linkedin_url\": \"https://www.linkedin.com/in/seansmith\",\n" +
         "        \"twitter_handle\": \"@seansmith\",\n" +
         "        \"location\": {\n" +
         "            \"city\": \"San Francisco\",\n" +
         "            \"state\": \"California\", \n" +
         "            \"country\": \"United States\"\n" +
         "        },\n" +
         "        \"work_phone\": \"+1 415-555-7890\",\n" +
         "        \"employment_history\": [\n" +
         "            {\n" +
         "                \"company\": \"Culinary Innovations\",\n" +
         "                \"job_title\": \"Sales Representative\",\n" +
         "                \"years\": \"2019-2022\"\n" +
         "            },\n" +
         "            {\n" +
         "                \"company\": \"Gourmet Solutions\",\n" +
         "                \"job_title\": \"Business Development Associate\",\n" +
         "                \"years\": \"2016-2019\"\n" +
         "            }\n" +
         "        ]\n" +
         "    },\n" +
         "    \"company\": {\n" +
         "        \"name\": \"Rich Table\",\n" +
         "        \"domain\": \"richtablesf.com\",\n" +
         "        \"industry\": \"Hospitality & Food Services\",\n" +
         "        \"sector\": \"Restaurant\",\n" +
         "        \"employee_count\": 45,\n" +
         "        \"annual_revenue\": \"$5M-$10M\",\n" +
         "        \"company_type\": \"Private\",\n" +
         "        \"headquarters\": {\n" +
         "            \"city\": \"San Francisco\",\n" +
         "            \"state\": \"California\",\n" +
         "            \"country\": \"United States\"\n" +
         "        },\n" +
         "        \"linkedin_url\": \"https://www.linkedin.com/company/rich-table\",\n" +
         "        \"twitter_handle\": \"@richtablesf\",\n" +
         "        \"facebook_url\": \"https://www.facebook.com/richtable\",\n" +
         "        \"technologies_used\": [\n" +
         "            \"OpenTable\",\n" +
         "            \"Square\",\n" +
         "            \"Yelp Reservations\",\n" +
         "            \"Toast POS\",\n" +
         "            \"Resy\"\n" +
         "        ],\n" +
         "        \"funding_info\": {\n" +
         "            \"total_funding\": \"$2M\",\n" +
         "            \"last_round\": \"Seed\",\n" +
         "            \"last_round_date\": \"2020-03-15\",\n" +
         "            \"investors\": [\n" +
         "                \"Local Food Ventures\",\n" +
         "                \"Bay Area Restaurant Group\"\n" +
         "            ]\n" +
         "        },\n" +
         "        \"key_decision_makers\": [\n" +
         "            {\n" +
         "                \"name\": \"Evan Rich\",\n" +
         "                \"title\": \"Co-Owner/Chef\",\n" +
         "                \"linkedin_url\": \"https://www.linkedin.com/in/evanrich\"\n" +
         "            },\n" +
         "            {\n" +
         "                \"name\": \"Sarah Rich\",\n" +
         "                \"title\": \"Co-Owner/Chef\",\n" +
         "                \"linkedin_url\": \"https://www.linkedin.com/in/sarahrich\"\n" +
         "            }\n" +
         "        ],\n" +
         "        \"hiring_trends\": {\n" +
         "            \"open_positions\": 3,\n" +
         "            \"growth_rate\": \"10% YoY\",\n" +
         "            \"top_hiring_departments\": [\n" +
         "                \"Kitchen Staff\",\n" +
         "                \"Service\",\n" +
         "                \"Management\"\n" +
         "            ]\n" +
         "        }\n" +
         "    }\n" +
         "}";
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

    // String email = ((JsonObject) arguments).values().get("email").asStringOrThrow();
    // String companyWebsite = ((JsonObject) arguments).values().get("company_website").asStringOrThrow();
    // String companyName = ((JsonObject) arguments).values().get("company_name").asStringOrThrow();
    // String title = ((JsonObject) arguments).values().get("job_title").asStringOrThrow();
    // String name = ((JsonObject) arguments).values().get("name").asStringOrThrow();

    // System.out.println(email + " " + companyWebsite);

    return "{\n" +
         "    \"contact\": {\n" +
         "        \"id\": \"0032D00000pQxyzQAC\",\n" +
         "        \"name\": \"Sean Smith\",\n" +
         "        \"email\": \"sean.smith@gmail.com\",\n" +
         "        \"title\": \"Account Executive\",\n" +
         "        \"phone\": null\n" +
         "    },\n" +
         "    \"account\": {\n" +
         "        \"id\": \"0012D000008QxyzQAC\", \n" +
         "        \"name\": \"Rich Table\",\n" +
         "        \"website\": \"https://www.richtablesf.com/\",\n" +
         "        \"industry\": \"Restaurant Technology\",\n" +
         "        \"annual_revenue\": null\n" +
         "    },\n" +
         "    \"lead\": {\n" +
         "        \"id\": \"00Q2D000001QxyzQAC\",\n" +
         "        \"status\": \"Open\",\n" +
         "        \"rating\": \"Cold\",\n" +
         "        \"source\": \"Demo Request\",\n" +
         "        \"converted\": false\n" +
         "    },\n" +
         "    \"interactions\": [\n" +
         "        {\n" +
         "            \"type\": \"Web Form Submission\",\n" +
         "            \"date\": \"2024-02-15T10:23:45Z\",\n" +
         "            \"notes\": \"Generic demo request with minimal product context\",\n" +
         "            \"campaign\": null\n" +
         "        }\n" +
         "    ],\n" +
         "    \"opportunity_notes\": \"Weak initial fit - product seems complex for potential customer's needs\"\n" +
         "}";
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

    return "Skip to main content\n" +
        "199 Gough St,  San Francisco, CA 94102\n" +
        "Food & Drink\n" +
        "Hours & Location\n" +
        "Private Events\n" +
        "Cookbook / Store\n" +
        "Gift Cards\n" +
        "Buy Gift Cards\n" +
        "Check Your Balance\n" +
        "Reservations\n" +
        "199 Gough St,  San Francisco, CA 94102\n" +
        "Toggle Navigation\n" +
        "Food & Drink\n" +
        "Hours & Location\n" +
        "Private Events\n" +
        "Cookbook / Store\n" +
        "Gift Cards\n" +
        "Buy Gift Cards\n" +
        "Check Your Balance\n" +
        "Contact\n" +
        "FAQ\n" +
        "Careers\n" +
        "Press\n" +
        "Story\n" +
        "Reservations Signup For Updates\n" +
        "Facebook\n" +
        "Twitter\n" +
        "Instagram\n" +
        "199 Gough St,  San Francisco, CA 94102\n" +
        "powered by BentoBox\n" +
        "Homepage\n" +
        "Main content starts here, tab to start navigating\n" +
        "Scroll Down to Content\n" +
        "Slide 1 of 4\n" +
        "Slide 2 of 4\n" +
        "Slide 3 of 4\n" +
        "Slide 4 of 4\n" +
        "hero gallery paused, press to play images slides\n" +
        "Playing hero gallery, press to pause images slides\n" +
        "Rich Table\n" +
        "Is the culinary vision of Chefs Evan and Sarah Rich. With over three decades of combined experience in San Francisco and New York high-end restaurants, the team brings with them a wealth of talent, knowledge of quality foods and wine, and connections with the best farms and purveyors.\n" +
        "Private Dining  \n" +
        "Host your next event with Rich Table.\n" +
        "Learn More\n" +
        "Join Our Team\n" +
        "Learn More\n" +
        "Visit Our Sister Restaurant\n" +
        "RT ROTISSERIE 101 Oak Street, San Francisco, CA 94102 11am-9pm Daily\n" +
        "Learn More\n" +
        "Reservations\n" +
        "Facebook\n" +
        "Twitter\n" +
        "Instagram\n" +
        "Contact\n" +
        "FAQ\n" +
        "Careers\n" +
        "Press\n" +
        "Story\n" +
        "Signup For Updates\n" +
        "powered by BentoBox\n" +
        "leave this field blank\n" +
        "Email Signup\n" +
        "Please, enter a valid first name\n" +
        "First Name - Required\n" +
        "Please, enter a valid last name\n" +
        "Last Name - Required\n" +
        "Please, enter a valid email\n" +
        "Email - Required\n" +
        "Submit\n" +
        "Please check errors in the form above\n" +
        "Thank you for signing up for email updates!\n" +
        "Close\n" +
        "This site is protected by reCAPTCHA and the Google\n" +
        "Privacy Policy and Terms of Service apply.\n" +
        "Reservations\n" +
        "Location - Required\n" +
        "Location\n" +
        "Rich Table\n" +
        "Please, select a location\n" +
        "Number of People - Optional\n" +
        "Number of People\n" +
        "1 Person\n" +
        "2 People\n" +
        "3 People\n" +
        "4 People\n" +
        "5 People\n" +
        "6 People\n" +
        "7 People\n" +
        "8+ People\n" +
        "Date - Required\n" +
        "Please, select a date\n" +
        "Time - Optional\n" +
        "Time\n" +
        "11:00 PM\n" +
        "10:30 PM\n" +
        "10:00 PM\n" +
        "9:30 PM\n" +
        "9:00 PM\n" +
        "8:30 PM\n" +
        "8:00 PM\n" +
        "7:30 PM\n" +
        "7:00 PM\n" +
        "6:30 PM\n" +
        "6:00 PM\n" +
        "5:30 PM\n" +
        "5:00 PM\n" +
        "4:30 PM\n" +
        "4:00 PM\n" +
        "3:30 PM\n" +
        "3:00 PM\n" +
        "2:30 PM\n" +
        "2:00 PM\n" +
        "1:30 PM\n" +
        "1:00 PM\n" +
        "12:30 PM\n" +
        "12:00 PM\n" +
        "11:30 AM\n" +
        "11:00 AM\n" +
        "10:30 AM\n" +
        "10:00 AM\n" +
        "9:30 AM\n" +
        "9:00 AM\n" +
        "8:30 AM\n" +
        "8:00 AM\n" +
        "7:30 AM\n" +
        "7:00 AM\n" +
        "Find A Table\n" +
        "Please check errors in the form above;";
  }
}
