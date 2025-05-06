package sdk.tools;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ToolRegistry {
    // Map: agentName → list of tools
    private static final Map<String, List<Tool>> agentToolMap = new HashMap<>();

    public static void register(String agentName, List<Tool> tools) {
        agentToolMap.put(agentName, tools);
    }

    public static List<Tool> getToolsForAgent(String agentName) {
        return agentToolMap.getOrDefault(agentName, List.of());
    }

    public static Optional<Tool> getToolForAgent(String agentName, String toolName) {
        return getToolsForAgent(agentName).stream()
                .filter(t -> t.getName().equals(toolName))
                .findFirst();
    }
}

