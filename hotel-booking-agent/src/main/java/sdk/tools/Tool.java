package sdk.tools;

import java.util.Map;

public interface Tool {
    String getName();                          // e.g., "getHotelReviews"
    String getDescription();                   // e.g., "Retrieve hotel reviews from past guests"
    Map<String, String> getParameters();       // param name → type (e.g., "hotel_id" → "string")
    String invoke(Map<String, Object> input);  // executes the tool with provided input
}
