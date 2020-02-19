package inclass;

import kstreams.exercise14.model.Tuple;
import kstreams.exercise14.model.TupleSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class Operations101 {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "shakespeare_stream" + UUID.randomUUID());
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> lines = builder.stream("shakespeare_topic",
                Consumed.with(Serdes.String(), Serdes.String()));


        KStream<String, String> words = lines.mapValues((readOnlyKey, value) -> {

            return value.toLowerCase()
                    .replace(".", " ")
                    .replace(",", " ")
                    .replace(":", " ")
                    .replace(";", " ")
                    .replace("?", " ")
                    .replace("\"", " ")
                    .replace("\'", " ");

        }).flatMapValues((readOnlyKey, value) -> {
            String[] s = value.split(" ");
            return Arrays.asList(s);
        }).filterNot((key, value) -> value.matches("[0-9]+"))
                .filterNot((key, value) -> value.isEmpty());

        KStream<String, String>[] branch = words.branch((key, value)
                -> value.length() % 2 == 0, (key, value) -> value.length() % 2 != 0);


        KStream<String, String> evensWord = branch[1];

        KStream<String, Integer> stringIntegerKStream = evensWord
                //from stream of words to stream of words' length
                .mapValues((readOnlyKey, value) -> value.length());

        stringIntegerKStream.print(Printed.toSysOut());

        KTable<String, Tuple<Integer, Integer>> aggregate1 = stringIntegerKStream
                //from streams of words' length to stream of tuples <0,length>
                .mapValues((readOnlyKey, value) -> new Tuple<>(0, value))
                //self explaining
                .groupByKey()
                //
                .aggregate(
                        //starting element for folding
                        () -> new Tuple<>(0, 0),
                        (key, value, aggregate) -> {
                            //aggregation logic
                            aggregate.t1++;
                            aggregate.t2 += value.t2;

                            return aggregate;
                            //configure deserialization and serialization
                        }, Materialized.with(Serdes.String(), new TupleSerde<>()));


        aggregate1
                .mapValues((readOnlyKey, value) -> value.t2 / value.t1)
                .toStream();


        KTable<String, Long> count = words
                .groupBy((key, value) -> value).count();

//        count.toStream().print(Printed.toSysOut());

        Topology build = builder.build();

        KafkaStreams app = new KafkaStreams(build, properties);

        System.out.println(build.describe());
        app.cleanUp();
        app.start();
    }
}
