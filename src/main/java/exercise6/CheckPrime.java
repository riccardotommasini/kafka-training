package exercise6;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.IntStream;

public class CheckPrime {

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
                "check-prime-consumer");


        KafkaConsumer<Integer, Integer> consumer =
                new KafkaConsumer<Integer, Integer>(properties);


        consumer.subscribe(Arrays.asList("sums"));

        consumer.poll(0);

        consumer.seekToBeginning(consumer.assignment());

        while (true) {

            ConsumerRecords<Integer, Integer> records =
                    consumer.poll(Duration.ofMillis(100));


            records.forEach(
                    r -> {

                        if (checkPrime(r.value())) {
                            System.out.println(r.value() + " is prime!");
                        }
                    });

            Thread.sleep(5000);
        }


    }


    public static boolean checkPrime(int numberToCheck) {
        return numberToCheck > 0 && IntStream.range(2, numberToCheck)
                .map(n -> numberToCheck % n)
                .noneMatch(value -> value == 0);
    }
}
