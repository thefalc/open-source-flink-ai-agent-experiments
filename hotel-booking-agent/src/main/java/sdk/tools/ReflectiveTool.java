package sdk.tools;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.LinkedHashMap;
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
        Map<String, String> result = new LinkedHashMap<>();

        Parameter[] params = method.getParameters();
        for (Parameter param : params) {
            result.put(param.getName(), param.getType().getSimpleName().toLowerCase());
        }

        return result;
    }

    @Override
    public String invoke(Map<String, Object> input) {
        System.out.println("tool invoke");
        try {
            Class<?>[] paramTypes = method.getParameterTypes();

            if (paramTypes.length == 1) {
                Class<?> paramType = paramTypes[0];

                // If the method expects a Map, pass the full input
                if (Map.class.isAssignableFrom(paramType)) {
                    return (String) method.invoke(targetInstance, input);
                }

                // If the method expects a single value, pass the first value from the map
                Object firstArg = input.values().stream().findFirst()
                        .orElseThrow(() -> new IllegalArgumentException("No argument provided"));

                return (String) method.invoke(targetInstance, firstArg);
            }

            throw new IllegalArgumentException("Unsupported tool signature for: " + method.getName());
        } catch (Exception e) {
            throw new RuntimeException("Error invoking tool: " + name, e);
        }
    }
}
