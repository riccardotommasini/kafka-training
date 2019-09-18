package exercise4;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class CheckPartitions {

    public static void main(String[] args) throws InterruptedException {

        Properties properties = new Properties();
        properties.setProperty(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        properties.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                IntegerDeserializer.class.getName());
        properties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                IntegerDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                "number-partition-checker");


        KafkaConsumer<Integer, Integer> consumer =
                new KafkaConsumer<Integer, Integer>(properties);


        consumer.subscribe(Arrays.asList("numbers2"));

        consumer.poll(0);

        consumer.seekToBeginning(consumer.assignment());

        while (true) {

            ConsumerRecords<Integer, Integer> records =
                    consumer.poll(Duration.ofMillis(100));


            records.forEach(
                    r -> {

                        System.out.println(r.partition());
                        System.out.println(r.key());
                        System.out.println(r.value());


                    });

            Thread.sleep(5000);

            //todo siamo riusciti a metterli nella giusta partizione?
        }
    }
}
