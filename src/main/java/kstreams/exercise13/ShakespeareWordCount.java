package kstreams.exercise13;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class ShakespeareWordCount {

    final static Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);


    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount" + UUID.randomUUID().toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStream<String, String> sentences = builder.stream("shakespeare_topic", Consumed.with(Serdes.String(), Serdes.String()));

        // words.print(Printed.toSysOut());
        // words.mapValues(String::length).print(Printed.toSysOut());
        //  words.flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase()))).print(Printed.toSysOut());
        KStream<String, String> words = sentences
                //removing symbols
                .mapValues(value ->
                        value.toLowerCase().replace(",", " ")
                                .replace(";", " ")
                                .replace(".", " ")
                                .replace("!", " ")
                                .replace("\"", " ")
                                .replace("?", " "))
                //splitting sentences into words
                .flatMapValues(value -> Arrays.asList(pattern.split(value)));

        KTable<String, Long> groupedWords = words
                //grouping by words
                .groupBy((poem, word) -> word).count();

        groupedWords.toStream().to("wordcount");

        KStream<String, String>[] branches = words.branch((k, v) -> v.startsWith("a"), (k1, v1) -> v1.startsWith("z"), (s, s2) -> true);

        branches[0].filter((s, s2) -> s.contains("Romeo and Juliet")).filterNot((o, w) -> w.length() < 3).print(Printed.toSysOut());

        Topology topology = builder.build();
        KafkaStreams ks = new KafkaStreams(topology, props);
        ks.cleanUp();
        ks.start();
    }


}



