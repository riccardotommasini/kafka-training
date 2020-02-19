package inclass;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Arrays;
import java.util.Properties;

public class WordCount {
    public static void main(String[] args) throws InterruptedException {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();
        // Serializers/deserializers (serde) for String and Long types
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

// Construct a `KStream` from the input topic "streams-plaintext-input", where message values
// represent lines of text (for the sake of this example, we ignore whatever may be stored
// in the message keys).
        KStream<String, String> textLines = builder.stream("words", Consumed.with(stringSerde, stringSerde));

        KTable<String, Long> wordCounts = textLines
                // Split each text line, by whitespace, into words.  The text lines are the message
                // values, i.e. we can ignore whatever data is in the message keys and thus invoke
                // `flatMapValues` instead of the more generic `flatMap`.
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                // We use `groupBy` to ensure the words are available as message keys
                .groupBy((key, value) -> value)
                // Count the occurrences of each word (message key).
                .count();

// Convert the `KTable<String, Long>` into a `KStream<String, Long>` and write to the output topic.
        wordCounts.toStream().print(Printed.toSysOut());

        Topology build = builder.build();
        MyKafkaStreams app = new MyKafkaStreams(build, props);
        System.out.println(build.describe());

        app.cleanUp();
        app.start();

        props.put("client.id", "java-admin-client");
        AdminClient client = AdminClient.create(props);

        while (true) {
            app.allMetadata().forEach(streamsMetadata -> {

                System.out.println(streamsMetadata);
                streamsMetadata.topicPartitions().forEach(System.out::println);

            });

            app.localThreadsMetadata().forEach(threadMetadata -> {

                System.out.println(threadMetadata);

                threadMetadata.activeTasks().forEach(
                        taskMetadata -> {
                            System.out.println(taskMetadata);
                            taskMetadata.topicPartitions().forEach(System.out::println);
                        }
                );
                threadMetadata.producerClientIds().forEach(System.out::println);
                System.out.println(threadMetadata.consumerClientId());

            });

            Arrays.stream(app.threads()).forEach(streamThread -> {

                streamThread.tasks().forEach((taskId, streamTask) -> {

                    System.out.println("TASK " + taskId);
                    System.out.println(streamTask);
                    System.out.println(streamTask.topology().toString());

                });

            });

            Thread.sleep(10000);
        }
    }
}