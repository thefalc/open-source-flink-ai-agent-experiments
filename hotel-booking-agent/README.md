# Multi-Agent Hotel Booking Engagement
This application uses Apache Flink, Apache Kafka, and OpenAI to create a hotel booking engagement agent.

The multi-agent system automates and personalizes an email campaign to someone who is on a hotel website, looked at bookings, but hasn't booked. Apache Flink and external model inference is used to orchestrate communication with a series of AI agents, each responsible for a specific task in the lead management and outreach process.

The system is event-driven, leveraging [Confluent Cloud's](https://www.confluent.io/) as the backbone for real-time communication and data flow between agents. Each agent is a microservice that runs as a Flink job using the [Table API](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/overview/).

## How it works
TODO

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

Go into your `hotel-booking-agent` folder and create a `.env` file with your OpenAI API key.

```bash
OPENAI_API_KEY=REPLACE_ME
```

Next, following the [instructions](https://docs.confluent.io/cloud/current/client-apps/config-client.html) to create a new Java client. Once the client downloads, unzip it and find the `client.properties` file. Copy this file into the /open-source-flink-ai-agent-experiments/hotel-booking-agent/src/main/resources folder.

### Setting up Confluent Cloud

The AI SDR uses Confluent Cloud to move data in real-time for communication between the agents.

#### Create the topics for agent communication and routing

TODO

### Run the application

1. In a terminal, navigate to your project directory. Run the app with the following command:
```shell
mvn clean package
flink run -c sdk.AgentJobRunner  target/ai-sdr-0.1.jar
```
2. In Confluent Cloud, navigate to the `hotel_insights_input` topic.
3. Click **Actions > Produce new message**
4. Paste the following into the **Value** field.
```json
{
  "context": "Customer Email: | hoyt.huel@gmail.com | Hotel ID: | H10000382 | Activity Time: | 2025-03-01 11:26:44.230 | Hotel Name: | River Nice Luxury Lodge | City: | Nice | Similar Hotels: | River Nice Spa | Reviews: | The hotel’s dedication to sustainability, evident in its operations and decor, added a meaningful layer to our stay.||| The custom-designed furniture and artwork throughout the hotel celebrated local craftsmanship, adding to the unique ambience.||| Walking through the hotel grounds felt like strolling through a meticulously designed botanical garden, enhancing our sense of tranquility.||| The hotel's music selection in the common areas created an uplifting and welcoming atmosphere. It was the perfect backdrop to our luxurious stay.||| Having access to a well-equipped exercise room made my stay even more enjoyable. It was great to have the option to unwind with some physical activity.||| The hotel's spa was a haven of relaxation, offering a serene escape with top-notch services. Coupled with the elegant ambiance, it was the highlight of our stay.||| The hotel's spa was a haven of relaxation, offering a serene escape with top-notch services. Coupled with the elegant ambiance, it was the highlight of our stay.||| The hotel's proximity to major tourist attractions was incredibly convenient. Being able to walk to iconic landmarks and museums enriched our travel experience, saving us time and allowing for spontaneous explorations. This location is ideal for travelers eager to immerse themselves in the city's culture. | LLM Response: | Here's a Python function that summarizes the reviews into a single sentence:\n\n```python\ndef summarize_reviews(reviews):\n    \"\"\"\n    Summarizes hotel reviews into a concise summary sentence.\n\n    Args:\n    reviews (str): A string containing one or more hotel reviews, delimited by '|||'.\n\n    Returns:\n    str: A summary sentence highlighting what customers liked most about the hotel.\n    \"\"\"\n\n    # Handle the case where reviews is an empty string\n    if not reviews.strip():\n        return \"NO REVIEWS FOUND.\"\n\n    # Split the reviews into a list\n    reviews = reviews.split('|||')\n\n    # Initialize an empty set to store keywords\n    keywords = set()\n\n    # Initialize an empty dictionary to store the frequency of keywords\n    keyword_frequency = {}\n\n    # Process each review\n    for review in reviews:\n        # Remove leading and trailing whitespace\n        review = review.strip()\n\n        # Split the review into sentences\n        sentences = review.split('. ')\n\n        # Process each sentence\n        for sentence in sentences:\n            # Remove punctuation and convert to lowercase\n            sentence = sentence.lower().replace('.', '').replace(',', '').replace('!', '')\n\n            # Tokenize the sentence into words\n            words = sentence.split()\n\n            # Iterate over the words"
}
```
5. Wait for the agent flow to complete. If everything goes well, after a few minutes you'll have an email campaign in the the `hotel_insights_output` topic.