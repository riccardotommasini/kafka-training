package exercise9;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;


public class TemperatureProducerAvro {
    public static void main(String[] args) throws InterruptedException {

        Random random = new Random(1);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:8081");

        KafkaProducer<AvroLocation, AvroObservation> termometer = new KafkaProducer<>(props);


        while (true) {

            int temp = random.nextInt(60) - 20;


            AvroLocationType lt = AvroLocationType.values()[random.nextInt(
                    AvroLocationType.values().length - 1)];

            AvroObservation obs = AvroObservation.newBuilder()
                    .setTimestamp(System.currentTimeMillis())
                    .setValue(temp)
                    .setObservationType(AvroObservationType.TEMPERATURE).build();

            AvroLocation loc = AvroLocation.newBuilder()
                    .setLocationArea(lt.name() + random.nextInt(4))
                    .setLocationType(lt).build();


            ProducerRecord<AvroLocation, AvroObservation> avro_temperature = new ProducerRecord<>("avro_temperature", loc, obs);

            termometer.send(avro_temperature);

            Thread.sleep(6000);

        }

    }

}
