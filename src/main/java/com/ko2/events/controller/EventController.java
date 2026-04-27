package com.ko2.events.controller;

import com.ko2.events.model.AlertEntry;
import com.ko2.events.model.AuditEntry;
import com.ko2.events.repository.AlertEntryRepository;
import com.ko2.events.repository.AuditEntryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;

/**
 * Endpoints REST para consultar auditoría y alertas.
 * Accesibles solo con JWT de ROLE_ADMIN (validado por SecurityConfig + JwtAuthenticationFilter).
 *
 * El API Gateway (puerto 7000) enruta /events/** → http://ko2-kafka-events:6000
 */
@RestController
@RequestMapping("/events")
@RequiredArgsConstructor
@PreAuthorize("hasRole('ADMIN')")
public class EventController {

    private final AuditEntryRepository auditEntryRepository;
    private final AlertEntryRepository alertEntryRepository;

    // ── Auditoría ─────────────────────────────────────────────────────────────

    /**
     * GET /events/audit
     *
     * Parámetros opcionales:
     *   ?username=javi        → filtra por usuario
     *   ?eventType=USER_LOGIN → filtra por tipo
     *   ?from=2024-01-01T00:00:00&to=2024-12-31T23:59:59 → rango de fechas
     *   ?page=0&size=20       → paginación
     */
    @GetMapping("/audit")
    public ResponseEntity<Page<AuditEntry>> getAudit(
            @RequestParam(required = false) String username,
            @RequestParam(required = false) String eventType,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        Pageable pageable = PageRequest.of(page, Math.min(size, 100));

        Page<AuditEntry> result;

        if (from != null && to != null) {
            result = auditEntryRepository.findByTimestampBetweenOrderByTimestampDesc(from, to, pageable);
        } else if (username != null) {
            result = auditEntryRepository.findByUsernameOrderByTimestampDesc(username, pageable);
        } else if (eventType != null) {
            result = auditEntryRepository.findByEventTypeOrderByTimestampDesc(eventType, pageable);
        } else {
            result = auditEntryRepository.findAllByOrderByTimestampDesc(pageable);
        }

        return ResponseEntity.ok(result);
    }

    // ── Alertas ───────────────────────────────────────────────────────────────

    /**
     * GET /events/alerts
     *
     * Parámetros opcionales:
     *   ?username=javi  → filtra alertas de un usuario concreto
     *   ?page=0&size=20 → paginación
     */
    @GetMapping("/alerts")
    public ResponseEntity<Page<AlertEntry>> getAlerts(
            @RequestParam(required = false) String username,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        Pageable pageable = PageRequest.of(page, Math.min(size, 100));

        Page<AlertEntry> result = (username != null)
                ? alertEntryRepository.findByUsernameOrderByDetectedAtDesc(username, pageable)
                : alertEntryRepository.findAllByOrderByDetectedAtDesc(pageable);

        return ResponseEntity.ok(result);
    }
}
