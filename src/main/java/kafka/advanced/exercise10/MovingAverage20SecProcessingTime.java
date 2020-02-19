package kafka.advanced.exercise10;

import kafka.advanced.exercise9.AvroLocationType;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;


public class MovingAverage20SecProcessingTime {
    public static void main(String[] args) throws InterruptedException {
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

            Supplier<Stream<ConsumerRecord<GenericRecord, GenericRecord>>>
                    supplier = () -> records.partitions().stream()
                    .flatMap(p -> records.records(p).stream())
                    .filter(r ->
                            AvroLocationType.ROOM
                                    .equals(r.key().get("locationType")));

            HashMap<GenericRecord, Set<Double>> map = supplier.get().collect(
                    () -> new HashMap<GenericRecord, Set<Double>>(),
                    (map2, g) -> {

                        if (!map2.containsKey(g.key())) {
                            map2.put(g.key(), new HashSet<>());
                        }
                        map2.get(g.key()).add((Double) g.value().get("value"));
                    },
                    HashMap::putAll);

//            HashMap<GenericRecord, IntStream> map = supplier.get().collect(
//                    () -> new HashMap<GenericRecord, IntStream>(),
//                    (map2, g) -> {
//
//                        if (!map2.containsKey(g.key())) {
//                            map2.put(g.key(), IntStream.empty());
//                        }
//                        IntStream intStream = map2.get(g.key());
//                        Integer value = (Integer) g.value().get("value");
//                        IntStream of = IntStream.of(value);
//                        map2.put(g.key(), IntStream.concat(of, intStream);
//
//                    },
//                    HashMap::putAll);

            map.entrySet().forEach(entry -> {

                int count = entry.getValue().size();
                Double sum =
                        entry.getValue()
                                .stream().reduce((a, b) -> a + b).orElse(0D);

                System.out.println("Avg of room [" +
                        entry.getKey().get("locationArea") +
                        "] is [" + sum / count+"]");
            });


            //wait 20 seconds
            Thread.sleep(20000);
        }
    }

}
