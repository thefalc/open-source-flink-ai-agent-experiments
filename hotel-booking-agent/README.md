# Multi-Agent for Hotel Engagement
This application uses Apache Flink, Apache Kafka, and OpenAI to create a MAS for engaging hotel customers.

The multi-agent system automates and personalizes an email campaign to someone who is on a hotel website, looked at bookings, but hasn't booked. 

The system is event-driven, leveraging [Confluent Cloud's](https://www.confluent.io/) as the backbone for real-time communication, orchestration, and data flow between agents. 

# What you'll need
In order to set up and run the application, you need the following:

* [Java 21](https://www.oracle.com/java/technologies/downloads/)
* [Flink v2.1](https://nightlies.apache.org/flink/flink-docs-master/)
* A [Confluent Cloud](https://www.confluent.io/) account
* An [OpenAI](https://openai.com/) API key

## Getting set up

### Get the starter code
In a terminal, clone the sample code to your project's working directory with the following command:

```shell
git clone https://github.com/thefalc/open-source-flink-ai-agent-experiments.git
```

### Setting up Apache Flink

* Downloads and install [Flink v2.1](https://nightlies.apache.org/flink/flink-docs-master/)
* Update /flink/conf/conf.yaml setting `taskmanager.numberOfTaskSlots: 4`
* Start the Flink cluster `/flink/bin/start-cluster.sh`

### Configure the AI SDR application

Go into your `multi-agent-sdr` folder and create a `.env` file with your OpenAI API key.

```bash
OPENAI_API_KEY=REPLACE_ME
```

Next, following the [instructions](https://docs.confluent.io/cloud/current/client-apps/config-client.html) to create a new Java client. Once the client downloads, unzip it and find the `client.properties` file. Copy this as `consumer.properties` and `producer.properties` into the /open-source-flink-ai-agent-experiments/multi-agent-sdr/src/main/resources folder.

### Run the application

1. In a terminal, navigate to your project directory. Run the app with the following command:
```shell
mvn clean package
flink run -c agents.AgentJobRunner  target/agent-runner-0.1.jar
```
2. In Confluent Cloud, navigate to the `hotel_insights_input` topic.
3. Click **Actions > Produce new message**
4. Paste the following into the **Value** field.
```json
{
  "context": "Customer Email: | hoyt.huel@gmail.com | Hotel ID: | H10000382 | Activity Time: | 2025-03-01 11:26:44.230 | Hotel Name: | River Nice Luxury Lodge | City: | Nice | Similar Hotels: | River Nice Spa | Reviews: | The hotel’s dedication to sustainability, evident in its operations and decor, added a meaningful layer to our stay.||| The custom-designed furniture and artwork throughout the hotel celebrated local craftsmanship, adding to the unique ambience.||| Walking through the hotel grounds felt like strolling through a meticulously designed botanical garden, enhancing our sense of tranquility.||| The hotel's music selection in the common areas created an uplifting and welcoming atmosphere. It was the perfect backdrop to our luxurious stay.||| Having access to a well-equipped exercise room made my stay even more enjoyable. It was great to have the option to unwind with some physical activity.||| The hotel's spa was a haven of relaxation, offering a serene escape with top-notch services. Coupled with the elegant ambiance, it was the highlight of our stay.||| The hotel's spa was a haven of relaxation, offering a serene escape with top-notch services. Coupled with the elegant ambiance, it was the highlight of our stay.||| The hotel's proximity to major tourist attractions was incredibly convenient. Being able to walk to iconic landmarks and museums enriched our travel experience, saving us time and allowing for spontaneous explorations. This location is ideal for travelers eager to immerse themselves in the city's culture."
}
```
4. Wait for the agent flow to complete. If everything goes well, after a few minutes you'll have a personalized email in the `hotel_insights_output` topic.