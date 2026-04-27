package com.ko2.events.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ko2.events.kafka.event.KafkaEvent;
import com.ko2.events.model.AuditEntry;
import com.ko2.events.repository.AuditEntryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Consumer de auditoría.
 * Persiste cada evento recibido (de cualquier topic KO2) en la tabla audit_events.
 *
 * groupId = "audit-group" → offset independiente del Alert consumer.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AuditConsumer {

    private final AuditEntryRepository auditEntryRepository;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = {"user.login", "currency.query", "weather.query"},
            groupId = "audit-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, KafkaEvent> record, Acknowledgment ack) {
        KafkaEvent event = record.value();

        if (event == null) {
            log.warn("Evento nulo recibido en topic {} — descartado", record.topic());
            ack.acknowledge();
            return;
        }

        try {
            AuditEntry entry = AuditEntry.builder()
                    .eventType(event.getEventType())
                    .username(event.getUsername())
                    .clientIp(event.getClientIp())
                    .timestamp(event.getTimestamp() != null
                            ? LocalDateTime.ofInstant(event.getTimestamp(), ZoneOffset.UTC)
                            : LocalDateTime.now(ZoneOffset.UTC))
                    .metadata(serializeMetadata(event))
                    .build();

            auditEntryRepository.save(entry);
            log.info("[AUDIT] {} | user={} | ip={} | topic={}",
                    entry.getEventType(), entry.getUsername(),
                    entry.getClientIp(), record.topic());

            ack.acknowledge();

        } catch (Exception e) {
            log.error("[AUDIT] Error procesando evento del topic {}: {}", record.topic(), e.getMessage(), e);
            // No se hace ack → Kafka reintentará en el siguiente poll
        }
    }

    private String serializeMetadata(KafkaEvent event) {
        if (event.getMetadata() == null || event.getMetadata().isEmpty()) {
            return "{}";
        }
        try {
            return objectMapper.writeValueAsString(event.getMetadata());
        } catch (JsonProcessingException e) {
            log.warn("No se pudo serializar metadata: {}", e.getMessage());
            return "{}";
        }
    }
}
