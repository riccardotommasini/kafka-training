package exercise3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Properties;

public class ProducerMain {

    public static void main(String[] args) throws InterruptedException {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                IntegerSerializer.class.getName());

        KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(properties);

        int i = 0;

        try {
            while (true) {
                i += 2;
                ProducerRecord<Integer, Integer> record =
                        new ProducerRecord<>("evens", i, i);
                producer.send(record);
                Thread.sleep(5000);
            }
        } finally {
            producer.close();
        }
    }
}
