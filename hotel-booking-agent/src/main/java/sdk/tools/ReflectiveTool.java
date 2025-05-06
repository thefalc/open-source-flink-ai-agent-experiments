package sdk.tools;

import java.lang.reflect.Method;
import java.util.Map;

public class ReflectiveTool implements Tool {
    private final Object targetInstance;
    private final Method method;
    private final String name;
    private final String description;

    public ReflectiveTool(Object targetInstance, Method method, String name, String description) {
        this.targetInstance = targetInstance;
        this.method = method;
        this.name = name;
        this.description = description;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public Map<String, String> getParameters() {
        // You can improve this by inspecting method signature or using annotations
        return Map.of(); // stub for now
    }

    @Override
    public String invoke(Map<String, Object> input) {
        try {
            return (String) method.invoke(targetInstance, input);
        } catch (Exception e) {
            throw new RuntimeException("Error invoking tool: " + name, e);
        }
    }
}
