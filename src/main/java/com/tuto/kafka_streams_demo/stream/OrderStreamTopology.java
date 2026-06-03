package com.tuto.kafka_streams_demo.stream;

import com.tuto.kafka_streams_demo.model.Order;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

@Configuration
@EnableKafkaStreams
public class OrderStreamTopology {

    private static final String INPUT_TOPIC  = "orders-topic";
    private static final String OUTPUT_TOPIC = "orders-enriched";

    @Bean
    public KStream<String, Order> ordersStream(StreamsBuilder builder) {

        JacksonJsonSerde<Order> orderSerde = new JacksonJsonSerde<>(Order.class);

        // 1. Source : lire le topic d'entrée
        KStream<String, Order> stream = builder.stream(
                INPUT_TOPIC,
                Consumed.with(Serdes.String(), orderSerde)
        );

        // 2. Filter : uniquement les commandes confirmées
        KStream<String, Order> confirmed = stream
                .filter((_, order) -> "CONFIRMED".equals(order.status()));

        // 3. MapValues : enrichir la commande
        KStream<String, Order> enriched = confirmed
                .mapValues(order -> new Order(
                        order.id(),
                        order.customerId(),
                        order.product().toUpperCase(),
                        order.amount(),
                        "PROCESSED"
                ));

        // 4. Sink : écrire sur le topic de sortie
        enriched.to(
                OUTPUT_TOPIC,
                Produced.with(Serdes.String(), orderSerde)
        );

        return confirmed;
    }
}

