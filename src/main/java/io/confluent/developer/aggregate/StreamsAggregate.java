package io.confluent.developer.aggregate;

import io.confluent.developer.StreamsUtils;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class StreamsAggregate {

    public static void main(String[] args) throws IOException {
        Properties streamsProps = StreamsUtils.loadProperties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-streams");

        StreamsBuilder builder = new StreamsBuilder();

        String inputTopic = streamsProps.getProperty("aggregate.input.topic");
        String outputTopic = streamsProps.getProperty("aggregate.output.topic");
        Map<String, Object> configMap = StreamsUtils.propertiesToMap(streamsProps);

        SpecificAvroSerde<ElectronicOrder> electronicSerde = StreamsUtils.getSpecificAvroSerde(configMap);

        KStream<String, ElectronicOrder> electronicStream =
                builder.stream(inputTopic, Consumed.with(Serdes.String(), electronicSerde))
                        .peek((key, value) -> System.out.println("Incoming record - key " +key +" value " + value));

        electronicStream.groupByKey().aggregate(() -> 0.0,
                (key, order, total) -> total + order.getPrice(), Materialized.with(Serdes.String(), Serdes.Double()))
                .toStream()
                .peek((key, value) -> System.out.println("Outgoing record - key " +key +" value " + value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Double()));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps);
        TopicLoader.runProducer();
        kafkaStreams.start();
    }


}
