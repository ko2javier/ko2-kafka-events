package com.ko2.events.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Alerta generada cuando un usuario supera el umbral de requests por minuto.
 * Tabla: alert_events
 */
@Entity
@Table(name = "alert_events", indexes = {
        @Index(name = "idx_alert_username", columnList = "username"),
        @Index(name = "idx_alert_detected_at", columnList = "detectedAt")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AlertEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 100)
    private String username;

    /** Cuántos requests acumuló en la ventana de 1 minuto. */
    @Column(nullable = false)
    private int requestCount;

    /** Inicio de la ventana deslizante que disparó la alerta. */
    @Column(nullable = false)
    private LocalDateTime windowStart;

    /** Momento en que se detectó y persistió la alerta. */
    @Column(nullable = false)
    private LocalDateTime detectedAt;
}
