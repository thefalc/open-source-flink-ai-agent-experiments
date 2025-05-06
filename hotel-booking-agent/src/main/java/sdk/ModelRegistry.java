package sdk;

import java.util.HashMap;
import java.util.Map;

public class ModelRegistry {
    private static final Map<String, ModelDefinition> models = new HashMap<>();

    public static void register(ModelDefinition model) {
        models.put(model.getName(), model);
    }

    public static ModelDefinition getModel(String name) {
        return models.get(name);
    }
}
