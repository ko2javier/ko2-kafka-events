package com.ko2.events.kafka.producer;

import com.ko2.events.kafka.event.KafkaEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Producer central del ecosistema KO2.
 * Los demás microservicios pueden inyectarlo si comparten el mismo Kafka,
 * o replicar estos mismos mensajes desde sus propios producers.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaEventProducer {

    private final KafkaTemplate<String, KafkaEvent> kafkaTemplate;

    @Value("${kafka.topics.user-login}")
    private String topicUserLogin;

    @Value("${kafka.topics.currency-query}")
    private String topicCurrencyQuery;

    @Value("${kafka.topics.weather-query}")
    private String topicWeatherQuery;

    // ── Métodos públicos ─────────────────────────────────────────────────────

    public void publishUserLogin(String username, String clientIp) {
        KafkaEvent event = KafkaEvent.builder()
                .eventType("USER_LOGIN")
                .username(username)
                .clientIp(clientIp)
                .timestamp(Instant.now())
                .metadata(Map.of())
                .build();
        send(topicUserLogin, username, event);
    }

    public void publishCurrencyQuery(String username, String clientIp,
                                     String fromCurrency, String toCurrency) {
        KafkaEvent event = KafkaEvent.builder()
                .eventType("CURRENCY_QUERY")
                .username(username)
                .clientIp(clientIp)
                .timestamp(Instant.now())
                .metadata(Map.of("from", fromCurrency, "to", toCurrency))
                .build();
        send(topicCurrencyQuery, username, event);
    }

    public void publishWeatherQuery(String username, String clientIp, String city) {
        KafkaEvent event = KafkaEvent.builder()
                .eventType("WEATHER_QUERY")
                .username(username)
                .clientIp(clientIp)
                .timestamp(Instant.now())
                .metadata(Map.of("city", city))
                .build();
        send(topicWeatherQuery, username, event);
    }

    // ── Internos ─────────────────────────────────────────────────────────────

    private void send(String topic, String key, KafkaEvent event) {
        CompletableFuture<SendResult<String, KafkaEvent>> future =
                kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Error publicando evento {} en topic {}: {}",
                        event.getEventType(), topic, ex.getMessage());
            } else {
                log.debug("Evento {} publicado → topic={} partition={} offset={}",
                        event.getEventType(), topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
