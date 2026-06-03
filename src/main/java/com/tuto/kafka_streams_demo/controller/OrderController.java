package com.tuto.kafka_streams_demo.controller;

import com.tuto.kafka_streams_demo.model.Order;
import com.tuto.kafka_streams_demo.producer.OrderProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderProducer producer;

    public OrderController(OrderProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public ResponseEntity<String> createOrder(@RequestBody Order order) {
        producer.sendOrder(order);
        return ResponseEntity.ok("Sent: " + order.id());
    }
}
