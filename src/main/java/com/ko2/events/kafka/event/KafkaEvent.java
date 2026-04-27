package com.ko2.events.kafka.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

/**
 * Evento genérico del ecosistema KO2.
 * Los tres servicios productores (auth, api-service) publican este mismo DTO.
 *
 * eventType: USER_LOGIN | CURRENCY_QUERY | WEATHER_QUERY
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaEvent {

    private String eventType;

    /** Username extraído del JWT o del login. */
    private String username;

    /** IP del cliente original (X-Forwarded-For desde el gateway). */
    private String clientIp;

    /** Momento exacto del evento (UTC). */
    @Builder.Default
    private Instant timestamp = Instant.now();

    /**
     * Datos adicionales según el tipo de evento:
     * - USER_LOGIN   → {}
     * - CURRENCY_QUERY → {"from":"USD","to":"EUR"}
     * - WEATHER_QUERY  → {"city":"Madrid"}
     */
    private Map<String, String> metadata;
}
