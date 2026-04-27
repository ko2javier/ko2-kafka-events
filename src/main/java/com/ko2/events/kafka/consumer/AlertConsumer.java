package com.ko2.events.kafka.consumer;

import com.ko2.events.kafka.event.KafkaEvent;
import com.ko2.events.model.AlertEntry;
import com.ko2.events.repository.AlertEntryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumer de alertas.
 *
 * Lógica: ventana deslizante de 1 minuto por usuario.
 * Si un usuario supera THRESHOLD requests en esa ventana → alerta persistida en MySQL.
 *
 * Estado en memoria: ConcurrentHashMap<username, Deque<Instant>>.
 * (Válido para una sola instancia; para multi-instancia usar Redis.)
 *
 * groupId = "alert-group" → consume los mismos eventos que audit-group
 * desde su propio offset, de forma completamente independiente.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AlertConsumer {

    private static final int THRESHOLD = 10;
    private static final long WINDOW_MILLIS = 60_000L; // 1 minuto

    private final AlertEntryRepository alertEntryRepository;

    /** Ventana deslizante por usuario: timestamps de los últimos requests. */
    private final ConcurrentHashMap<String, Deque<Instant>> requestWindows =
            new ConcurrentHashMap<>();

    @KafkaListener(
            topics = {"user.login", "currency.query", "weather.query"},
            groupId = "alert-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, KafkaEvent> record, Acknowledgment ack) {
        KafkaEvent event = record.value();

        if (event == null || event.getUsername() == null) {
            ack.acknowledge();
            return;
        }

        try {
            String username = event.getUsername();
            Instant now = event.getTimestamp() != null ? event.getTimestamp() : Instant.now();

            int countInWindow = recordAndCount(username, now);

            if (countInWindow > THRESHOLD) {
                persistAlert(username, countInWindow, now);
            }

            ack.acknowledge();

        } catch (Exception e) {
            log.error("[ALERT] Error procesando evento del topic {}: {}", record.topic(), e.getMessage(), e);
        }
    }

    /**
     * Registra el timestamp del nuevo request y devuelve cuántos hay en la ventana.
     * Thread-safe mediante computeIfAbsent + sincronización sobre el Deque.
     */
    private int recordAndCount(String username, Instant now) {
        Deque<Instant> window = requestWindows.computeIfAbsent(username, k -> new ArrayDeque<>());

        synchronized (window) {
            Instant cutoff = now.minusMillis(WINDOW_MILLIS);

            // Eliminar entradas fuera de la ventana
            while (!window.isEmpty() && window.peekFirst().isBefore(cutoff)) {
                window.pollFirst();
            }

            window.addLast(now);
            return window.size();
        }
    }

    private void persistAlert(String username, int count, Instant now) {
        LocalDateTime windowStart = LocalDateTime.ofInstant(
                now.minusMillis(WINDOW_MILLIS), ZoneOffset.UTC);
        LocalDateTime detectedAt = LocalDateTime.ofInstant(now, ZoneOffset.UTC);

        AlertEntry alert = AlertEntry.builder()
                .username(username)
                .requestCount(count)
                .windowStart(windowStart)
                .detectedAt(detectedAt)
                .build();

        alertEntryRepository.save(alert);
        log.warn("[ALERT] Usuario {} superó el umbral: {} requests en 1 minuto", username, count);
    }
}
