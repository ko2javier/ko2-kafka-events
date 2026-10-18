package com.ko2.events.config;

import com.ko2.events.kafka.event.KafkaEvent;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Para las metricas del cliente de Kafka. Ver el comentario de
     * {@link #consumerFactory()}: sin esto no existe el consumer lag.
     */
    private final MeterRegistry meterRegistry;

    // ── Topics ──────────────────────────────────────────────────────────────

    @Bean
    public NewTopic topicUserLogin() {
        return TopicBuilder.name("user.login").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic topicCurrencyQuery() {
        return TopicBuilder.name("currency.query").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic topicWeatherQuery() {
        return TopicBuilder.name("weather.query").partitions(3).replicas(1).build();
    }

    // ── Producer ─────────────────────────────────────────────────────────────

    @Bean
    public ProducerFactory<String, KafkaEvent> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        DefaultKafkaProducerFactory<String, KafkaEvent> factory = new DefaultKafkaProducerFactory<>(props);
        // Mismo motivo que en consumerFactory(): al construir la fabrica a
        // mano, Spring Boot no le engancha ningun listener de Micrometer.
        factory.addListener(new MicrometerProducerListener<>(meterRegistry));
        return factory;
    }

    @Bean
    public KafkaTemplate<String, KafkaEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // ── Consumer — fábrica compartida, groupId definido en cada @KafkaListener ─

    @Bean
    public ConsumerFactory<String, KafkaEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.ko2.events.kafka.event");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, KafkaEvent.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        DefaultKafkaConsumerFactory<String, KafkaEvent> factory =
                new DefaultKafkaConsumerFactory<>(props);
        // 2026-10-18. Sin esta linea NO EXISTE el consumer lag.
        //
        // Spring Boot engancha un MicrometerConsumerListener solo a la fabrica
        // que autoconfigura el; esta se construye a mano, asi que las metricas
        // del CLIENTE de Kafka (kafka_consumer_fetch_manager_records_lag_max
        // entre ellas) no se publicaban. Lo que si se publicaba,
        // spring_kafka_listener_seconds_*, mide cuanto tarda el listener en
        // procesar un mensaje, que no es lo mismo: un consumidor que se cae o
        // que se queda atras no se notaba en Prometheus.
        factory.addListener(new MicrometerConsumerListener<>(meterRegistry));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, KafkaEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        // ack manual → commit solo tras procesamiento exitoso
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(2);
        return factory;
    }
}
