package sdk.tools;

import sdk.annotations.ToolFunction;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class ToolExtractor {
    public static List<Tool> extractTools(Object agentInstance) {
        List<Tool> tools = new ArrayList<>();
        for (Method method : agentInstance.getClass().getDeclaredMethods()) {
            if (method.isAnnotationPresent(ToolFunction.class)) {
                ToolFunction meta = method.getAnnotation(ToolFunction.class);
                tools.add(new ReflectiveTool(agentInstance, method, meta.name(), meta.description()));
            }
        }
        return tools;
    }
}

