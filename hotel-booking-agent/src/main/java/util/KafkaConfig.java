package util;

import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaConfig {
    private final String bootstrapServers;
    private final String username;
    private final String password;

    public KafkaConfig(String propertiesFileName) {
        Properties config = new Properties();

        try (InputStream stream = getClass().getClassLoader().getResourceAsStream(propertiesFileName)) {
            if (stream == null) {
                throw new IllegalArgumentException("Properties file not found: " + propertiesFileName);
            }
            config.load(stream);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load Kafka config", e);
        }

        this.bootstrapServers = config.getProperty("bootstrap.servers");
        String jaasConfig = config.getProperty("sasl.jaas.config");

        this.username = extractJaasValue(jaasConfig, "username");
        this.password = extractJaasValue(jaasConfig, "password");
    }

    private String extractJaasValue(String jaasConfig, String key) {
        Pattern pattern = Pattern.compile(key + "='([^']*)'");
        Matcher matcher = pattern.matcher(jaasConfig);
        return matcher.find() ? matcher.group(1) : null;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}

