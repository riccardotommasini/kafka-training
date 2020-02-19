package kafka.exercise6;

import kafka.exercise5.MyPartitioner;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class GenPrimeAlt {

    public static void main(String[] args) throws InterruptedException {

        Random random = new Random();

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "sums-consumer");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        KafkaConsumer<Integer, Integer> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList("numbers2"));

        Properties properties2 = new Properties();
        properties2.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties2.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties2.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties2.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class.getName());

        KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(properties2);

        while (true) {

            ConsumerRecords<Integer, Integer> records =
                    consumer.poll(Duration.ofMillis(100));

            Supplier<Stream<Integer>> supplier = () -> records.partitions()
                    .stream()
                    .flatMap(topicPartition -> records.records(topicPartition).stream())
                    .map(ConsumerRecord::value);


//            Optional<Integer> reduce = integerStream.reduce(SumNumbers::primeNumbersTill);

            Optional<Integer> max = supplier.get().max(Integer::compareTo);
            Optional<Integer> min = supplier.get().min(Integer::compareTo);

            Optional<Integer> reduce = max.flatMap(n -> min.map(m -> primeNumbersTill(m, n)));

            Integer integer = reduce.orElse(random.nextInt(100));

            System.out.println("Sending " + integer);

            ProducerRecord<Integer, Integer> record =
                    new ProducerRecord<>("sums", integer, integer);
            producer.send(record);


            Thread.sleep(5000);
        }

    }

    public static Integer primeNumbersTill(int n, int m) {
        return IntStream.rangeClosed(n, m)
                .filter(GenPrimeAlt::isPrime).findFirst().orElse(n);
    }

    private static boolean isPrime(int x) {
        return IntStream.rangeClosed(2, (int) (Math.sqrt(x)))
                .filter(n -> (n & 0X1) != 0)
                .allMatch(n -> x % n != 0);
    }
}
