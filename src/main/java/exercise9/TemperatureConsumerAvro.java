package exercise9;

import exercise7.model.Location;
import exercise7.model.Observation;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;


public class TemperatureConsumerAvro {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "avroconsumer1");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        // TODO: Create a new consumer, with the properties we've created above

        Consumer<GenericRecord, GenericRecord> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("avro_temperature"));

        //todo explain 0 does not work.
        consumer.poll(1);
        Set<TopicPartition> assignment = consumer.assignment();
        consumer.seekToBeginning(assignment);


        while (true) {
            ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofMillis(100));

            records.forEach(record -> {

                System.out.println(record.offset());
                System.out.println(record.key());
                System.out.println(record.value().getSchema());
                record.value().getSchema().getFields().forEach(System.out::println);
                System.out.println(record.timestampType() + ": " + record.timestamp());


            });
            // TODO: Read records using the poll() method

            // TODO: Loop around the records, printing out each record.offset, record.key, and record.value
        }
    }

}
