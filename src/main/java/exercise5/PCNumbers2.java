package exercise5;

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

public class PCNumbers2 {

    public static void main(String[] args) throws InterruptedException {


        Properties properties = new Properties();
        properties.setProperty(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                IntegerDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                "number2-consumer");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                IntegerDeserializer.class.getName());

        KafkaConsumer<Integer, Integer> consumer =
                new KafkaConsumer<Integer, Integer>(properties);

        consumer.subscribe(Arrays.asList("evens", "odds"));

        consumer.poll(0);

        consumer.seekToBeginning(consumer.assignment());

        Properties properties2 = new Properties();
        properties2.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        properties2.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                IntegerSerializer.class.getName());
        properties2.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                IntegerSerializer.class.getName());
        properties2.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class.getName());


        KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(properties2);

        while (true) {

            ConsumerRecords<Integer, Integer> records =
                    consumer.poll(Duration.ofMillis(100));


            records.forEach(
                    r -> {

                        ProducerRecord<Integer, Integer> record =
                                new ProducerRecord<>("numbers2", r.value(), r.value());
                        producer.send(record);

                    });

            Thread.sleep(5000);
        }
    }
}
