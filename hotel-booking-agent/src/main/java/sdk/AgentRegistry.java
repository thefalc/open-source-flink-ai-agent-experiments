package sdk;

import sdk.tools.Tool;
import sdk.tools.ToolExtractor;
import sdk.tools.ToolRegistry;

import java.util.List;
import java.util.ArrayList;

// AgentRegistry provides access to all registered agents
public class AgentRegistry {
    private static final List<Agent> REGISTERED_AGENTS = new ArrayList<>();

    public static void register(Agent agent) {
        REGISTERED_AGENTS.add(agent);

        List<Tool> tools = ToolExtractor.extractTools(agent);
        ToolRegistry.register(agent.getName(), tools);
    }

    public static List<Agent> getRegisteredAgents() {
        return REGISTERED_AGENTS;
    }
}