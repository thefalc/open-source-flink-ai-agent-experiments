package sdk;

public class ModelDefinition {
    private final String name;
    private final String provider;
    private final String systemPrompt;

    public ModelDefinition(String name, String provider, String systemPrompt) {
        this.name = name;
        this.provider = provider;
        this.systemPrompt = systemPrompt;
    }

    public String getName() {
        return name;
    }

    public String getProvider() {
        return provider;
    }

    public String getSystemPrompt() {
        return systemPrompt;
    }
}
