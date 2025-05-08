package agents;

import java.util.List;
import java.util.Map;

import sdk.AgentRegistry;
import sdk.Agent;
import sdk.ModelDefinition;
import sdk.tools.Tool;
import sdk.annotations.ToolFunction;

public class HotelInsightsAgent implements Agent {
    static {
        AgentRegistry.register(new HotelInsightsAgent());
    }

    private final ModelDefinition modelDefinition;

    public HotelInsightsAgent() {
        modelDefinition = new ModelDefinition("hotel_insights_model", "gpt-4o", getSystemMessage());
    }

    @Override
    public String getName() {
        return "HotelInsightsAgent";
    }

    @Override
    public String getSystemMessage() {
        return "You're a Hotel Insights Specialist at River Hotels, a global hospitality brand operating\n"
                + "in over 40 countries. River Hotels is dedicated to delivering exceptional guest experiences\n"
                + "through smart marketing and real-time personalization.\n\n"
                + "Your role is to analyze the current hotel's offerings in relation to a guest's Customer\n"
                + "Research Report and generate a Hotel Research Report. This report will highlight how the\n"
                + "hotel's amenities, services, and experiences align with the guest's preferences, ensuring\n"
                + "tailored recommendations and a personalized stay.";
    }

    @Override
    public ModelDefinition getModel() {
        return modelDefinition;
    }

    @Override
    public String getInputTopic() {
        return "hotel_insights_input";
    }

    @ToolFunction(name = "getHotelReviews", description = "Retrieve hotel reviews for a given hotel ID")
    public String getHotelReviews(String hotelId) {
        System.out.println("getHotelReviews");
        return "No reviews found";
    }

    @ToolFunction(name = "getHotelAmenities", description = "Gets a list of the hotel amenities")
    public String getHotelAmenities(String hotelId) {
        System.out.println("getHotelAmenities");
        return "They have a ferris wheel";
    }

    @Override
    public List<String> getHandoffs() {
        return List.of("hotel_insights_output");
    }

    @Override
    public String getPrompt(String context) {
        StringBuilder sb = new StringBuilder();

        sb.append("Using the guest's Customer Research Report, generate a Hotel Research Report that evaluates how the current ")
                .append("hotel's offerings align with the guest's preferences and booking behavior. This report will help River Hotels ")
                .append("deliver personalized recommendations, room assignments, and service enhancements tailored to the guest's expectations.\n\n");

        sb.append("Key Responsibilities:\n")
                .append("- Analyze the guest's preferences based on their Customer Research Report, including travel patterns, room choices, and amenity usage.\n")
                .append("- Evaluate the current hotel's offerings, identifying relevant room types, available services, and exclusive experiences.\n")
                .append("- Compare hotel reviews to past guest preferences, ensuring the stay aligns with expectations.\n")
                .append("- Highlight personalized recommendations, such as room upgrades, service add-ons, or special offers that enhance the guest experience.\n")
                .append("- Identify potential gaps, such as unavailable preferred amenities, and suggest alternatives to maintain high satisfaction.\n\n");

        sb.append("Use dedicated tools to enhance personalization and optimize engagement:\n")
                .append("- Hotel Reviews - Analyzes feedback from past guests to assess strengths, weaknesses, and areas for improvement.\n")
                .append("- Hotel Amenities - Retrieves information on available room types, dining options, spa services, fitness facilities, and other key offerings.\n\n");

        sb.append("Ensure a clear and actionable CTA, encouraging the lead to engage without high friction.\n\n");

        sb.append(String.format("Customer Research Report: %s\n\n", context));

        sb.append("Expected Output - Hotel Research Report:\n")
                .append("The report should be concise, actionable, and aligned with the guest's needs, containing:\n")
                .append("- Guest Preference Alignment - How well the current hotel matches the guest's past stay preferences.\n")
                .append("- Room & View Recommendation - Best available options based on past room type, view, and bed configuration choices.\n")
                .append("- Amenities & Services Match - Available hotel amenities that align with the guest's usage history.\n")
                .append("- Guest Experience Insights - Any potential experience gaps and recommendations to improve satisfaction.\n")
                .append("- Personalized Stay Enhancements - Suggested perks, promotions, or personalized touches to maximize guest comfort and loyalty.\n\n");

        sb.append("This report will enable River Hotels to deliver a seamless, customized guest experience, increasing satisfaction and direct bookings while reinforcing brand loyalty.\n\n");

        sb.append("Output Format:\n")
                .append("- The output must be strictly formatted as JSON, with no additional text, commentary, or explanation.\n")
                .append("- The JSON should exactly match the following structure:\n\n");

        sb.append("{\n")
                .append("  \"guest_id\": \"123456\",\n")
                .append("  \"hotel_id\": \"RH-TOKYO-001\",\n")
                .append("  \"hotel_name\": \"River Grand Tokyo\",\n")
                .append("  \"location\": \"Tokyo, Japan\",\n")
                .append("  \"hotel_and_guest_research_report\": {\n")
                .append("    \"guest_preference_alignment\": {\n")
                .append("      \"room_match_score\": \"90\",\n")
                .append("      \"amenities_match_score\": \"85\",\n")
                .append("      \"overall_alignment\": \"Strong match with the guest's past stay preferences.\"\n")
                .append("    },\n")
                .append("    \"room_and_view_recommendation\": {\n")
                .append("      \"recommended_room_type\": \"Executive Suite\",\n")
                .append("      \"reason_for_recommendation\": \"Guest prefers King Bed and City View, and frequently stays in premium rooms.\",\n")
                .append("      \"available_views\": [\"City View\"],\n")
                .append("      \"bed_configuration\": \"One King Bed\"\n")
                .append("    },\n")
                .append("    \"amenities_and_services_match\": {\n")
                .append("      \"matching_amenities\": [\"Spa\", \"Executive Lounge\", \"Gym\"],\n")
                .append("      \"unavailable_amenities\": [\"Private Beach Access\"],\n")
                .append("      \"recommended_alternatives\": [\"Rooftop Infinity Pool instead of Private Beach Access\"]\n")
                .append("    },\n")
                .append("    \"guest_experience_insights\": {\n")
                .append("      \"potential_gaps\": [\n")
                .append("        {\n")
                .append("          \"issue\": \"Preferred amenity (Private Beach Access) is not available.\",\n")
                .append("          \"suggestion\": \"Offer complimentary spa treatment or priority poolside cabana reservation.\"\n")
                .append("        }\n")
                .append("      ],\n")
                .append("      \"guest_sentiment_analysis\": {\n")
                .append("        \"recent_reviews_match_guest_preferences\": \"true\",\n")
                .append("        \"notable_review_highlights\": [\n")
                .append("          \"Guests love the service in the Executive Lounge.\",\n")
                .append("          \"High ratings for cleanliness and staff hospitality.\"\n")
                .append("        ],\n")
                .append("        \"areas_for_improvement\": [\n")
                .append("          \"Some guests found room service to be slow during peak hours.\"\n")
                .append("        ]\n")
                .append("      }\n")
                .append("    },\n")
                .append("    \"personalized_stay_enhancements\": [\n")
                .append("      {\n")
                .append("        \"enhancement\": \"Complimentary Room Upgrade\",\n")
                .append("        \"details\": \"Upgrade to a Suite with Lounge Access as a loyalty perk.\",\n")
                .append("        \"justification\": \"Guest has redeemed room upgrades in the past and prefers premium accommodations.\"\n")
                .append("      },\n")
                .append("      {\n")
                .append("        \"enhancement\": \"Exclusive Spa Package\",\n")
                .append("        \"details\": \"Offer 20% off on spa services during the stay.\",\n")
                .append("        \"justification\": \"Guest frequently uses spa services and enjoys wellness amenities.\"\n")
                .append("      }\n")
                .append("    ]\n")
                .append("  }\n")
                .append("}\n\n");

        sb.append("Failure to strictly follow this format will result in incorrect output.");

        return sb.toString();
    }
}
