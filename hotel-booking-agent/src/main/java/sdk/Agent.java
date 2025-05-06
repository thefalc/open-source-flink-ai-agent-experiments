package sdk;

import java.util.List;

public interface Agent {
    /**
     * Unique agent name (used for registry, tracing, etc.)
     */
    String getName();

    /**
     * System message or role instruction (e.g., "You're a lead qualification assistant")
     */
    String getSystemMessage();

    /**
     * Name of the model this agent uses
     */
    ModelDefinition getModel();

    /**
     * Kafka topic this agent listens to for input events
     */
    String getInputTopic();

    /**
     * Topics or downstream systems this agent can hand off to
     */
    List<String> getHandoffs();

    /**
     * Generates a prompt for the model from a raw context string (usually JSON)
     */
    String getPrompt(String context);
}