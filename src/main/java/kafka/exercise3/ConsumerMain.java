package kafka.exercise3;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerMain {

    public static void main(String[] args) throws InterruptedException {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "even-consumer-odds-producer");


        KafkaConsumer<Integer, Integer> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList("evens"));

        consumer.poll(0);

        consumer.seekToBeginning(consumer.assignment());

        Properties properties2 = new Properties();
        properties2.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        properties2.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                IntegerSerializer.class.getName());
        properties2.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                IntegerSerializer.class.getName());

        KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(properties2);

        while (true) {

            ConsumerRecords<Integer, Integer> records =
                    consumer.poll(Duration.ofMillis(100));

            records.forEach(
                    r -> {

                        System.out.println(r.timestamp());
                        System.out.println(r.offset());
                        System.out.println(r.partition());
                        System.out.println(r.key());
                        System.out.println(r.value());

                        ProducerRecord<Integer, Integer> record =
                                new ProducerRecord<>("odds", r.key(), r.value() + 1);
                        producer.send(record);

                    });

            Thread.sleep(5000);
        }
    }
}
