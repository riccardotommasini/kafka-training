package inclass;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class MyStreamApp {

    public static void main(String[] args) throws InterruptedException {

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "myfirst" + UUID.randomUUID());
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> evens = builder.stream("evens4",
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, Integer> stringIntegerKStream = evens.mapValues((readOnlyKey, value) -> {
                    System.out.println(value);
                    return Integer.parseInt(value);
                }

        );

        stringIntegerKStream.mapValues((key, value) -> value + 1)
                .groupByKey().count().toStream()
                .mapValues((readOnlyKey, value) -> value.intValue())
                .to("counteven",
                        Produced.with(Serdes.String(),
                                Serdes.Integer()));

        Topology build = builder.build();

        MyKafkaStreams app = new MyKafkaStreams(build, properties);
        System.out.println(build.describe());

        app.cleanUp();
        app.start();

        properties.put("client.id", "java-admin-client");
        AdminClient client = AdminClient.create(properties);

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
