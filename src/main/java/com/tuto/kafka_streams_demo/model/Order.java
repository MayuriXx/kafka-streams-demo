package com.tuto.kafka_streams_demo.model;

public record Order(
        String id,
        String customerId,
        String product,
        double amount,
        String status  // "PENDING", "CONFIRMED", "CANCELLED"
) {}
