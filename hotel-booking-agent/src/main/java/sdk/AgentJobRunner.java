package sdk;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

import io.github.cdimascio.dotenv.Dotenv;
import util.KafkaConfig;
import tools.AgentTools;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;

public class AgentJobRunner {
    public static void main(String[] args) throws Exception {
        System.out.println("here");

        AgentTools.bootstrapAgents("agents");

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
        String apiKey = dotenv.get("OPENAI_API_KEY");

        System.out.println(apiKey);

        KafkaConfig kafkaConfig = new KafkaConfig("client.properties");
        String bootstrapServers = kafkaConfig.getBootstrapServers();
        String username = kafkaConfig.getUsername();
        String password = kafkaConfig.getPassword();

        List<Agent> agents = AgentRegistry.getRegisteredAgents();

        System.out.println("total agents: " + agents.size());

        for (Agent agent : agents) {
            // Register the model for each agent
            ModelRegistry.register(agent.getModel());

            String inputTopic = agent.getInputTopic();
            List<String> handoffs = agent.getHandoffs();

            System.out.println(inputTopic);

            // Define the input for the agent
            tEnv.executeSql(
                    "CREATE TABLE `" + inputTopic + "` (\n" +
                            "raw_data STRING\n" +
                            ") WITH (\n" +
                            "  'connector' = 'kafka',\n" +
                            "  'topic' = '" + inputTopic + "',\n" +
                            "  'properties.bootstrap.servers' = '" + bootstrapServers + "',\n" +
                            "  'properties.security.protocol' = 'SASL_SSL',\n" +
                            "  'properties.sasl.mechanism' = 'PLAIN',\n" +
                            "  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                            + username + "\" password=\"" + password + "\";',\n" +
                            "  'scan.startup.mode' = 'latest-offset',\n" +
                            "  'value.format' = 'raw'\n" +
                            ")");

            for (String outputTopic : handoffs) {
                // Define the topic to write the agent output to
                tEnv.executeSql(
                        "CREATE TABLE `" + outputTopic + "` (\n" +
                                "raw_data STRING\n" +
                                ") WITH (\n" +
                                "  'connector' = 'kafka',\n" +
                                "  'topic' = '" + outputTopic + "',\n" +
                                "  'properties.bootstrap.servers' = '" + bootstrapServers + "',\n" +
                                "  'properties.security.protocol' = 'SASL_SSL',\n" +
                                "  'properties.sasl.mechanism' = 'PLAIN',\n" +
                                "  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                                + username + "\" password=\"" + password + "\";',\n" +
                                "  'scan.startup.mode' = 'latest-offset',\n" +
                                "  'value.format' = 'raw'\n" +
                                ")");

                // Process all incoming events and write the agent output to the outgoing topic
                tEnv.createTemporarySystemFunction("AgentInferenceFunction", new AgentInferenceFunction(apiKey, agent.getName()));
                tEnv.from(inputTopic).select(call("AgentInferenceFunction", $("raw_data")))
                        .insertInto(outputTopic).execute();
            }
        }
    }
}
