package com.ko2.events.controller;

import com.ko2.events.model.AlertEntry;
import com.ko2.events.model.AuditEntry;
import com.ko2.events.repository.AlertEntryRepository;
import com.ko2.events.repository.AuditEntryRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
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
@Tag(name = "Events", description = "Consulta de auditoría y alertas de rate-limiting. Requiere JWT de ROLE_ADMIN.")
@SecurityRequirement(name = "bearerAuth")
public class EventController {

    private final AuditEntryRepository auditEntryRepository;
    private final AlertEntryRepository alertEntryRepository;

    // ── Auditoría ─────────────────────────────────────────────────────────────

    @Operation(
            summary = "Listado de eventos de auditoría",
            description = """
                    Devuelve todos los eventos procesados por los consumers Kafka (USER_LOGIN, CURRENCY_QUERY, WEATHER_QUERY), \
                    ordenados por timestamp descendente y paginados.

                    Los filtros son **mutuamente excluyentes** (se aplica el primero que coincida):
                    1. `from` + `to` — rango de fechas ISO-8601
                    2. `username` — eventos de un usuario concreto
                    3. `eventType` — eventos de un tipo concreto
                    4. Sin filtro — devuelve todos
                    """,
            responses = {
                    @ApiResponse(responseCode = "200", description = "Página de registros de auditoría",
                            content = @Content(schema = @Schema(implementation = Page.class))),
                    @ApiResponse(responseCode = "401", description = "JWT ausente o inválido", content = @Content),
                    @ApiResponse(responseCode = "403", description = "El JWT no tiene ROLE_ADMIN", content = @Content)
            }
    )
    @GetMapping("/audit")
    public ResponseEntity<Page<AuditEntry>> getAudit(
            @Parameter(in = ParameterIn.QUERY, description = "Filtra por username exacto", example = "javi")
            @RequestParam(required = false) String username,

            @Parameter(in = ParameterIn.QUERY, description = "Filtra por tipo de evento", example = "USER_LOGIN",
                    schema = @Schema(allowableValues = {"USER_LOGIN", "CURRENCY_QUERY", "WEATHER_QUERY"}))
            @RequestParam(required = false) String eventType,

            @Parameter(in = ParameterIn.QUERY, description = "Inicio del rango (ISO-8601). Requiere `to`.", example = "2025-01-01T00:00:00")
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,

            @Parameter(in = ParameterIn.QUERY, description = "Fin del rango (ISO-8601). Requiere `from`.", example = "2025-12-31T23:59:59")
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to,

            @Parameter(in = ParameterIn.QUERY, description = "Número de página (0-based)", example = "0")
            @RequestParam(defaultValue = "0") int page,

            @Parameter(in = ParameterIn.QUERY, description = "Registros por página (máx. 100)", example = "20")
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

    @Operation(
            summary = "Listado de alertas de rate-limiting",
            description = """
                    Devuelve las alertas generadas cuando un usuario supera el umbral de 10 requests/minuto. \
                    Ordenadas por fecha de detección descendente y paginadas.

                    Las alertas se generan en memoria con una ventana deslizante de 60 segundos \
                    (AlertConsumer, groupId: `alert-group`).
                    """,
            responses = {
                    @ApiResponse(responseCode = "200", description = "Página de alertas",
                            content = @Content(schema = @Schema(implementation = Page.class))),
                    @ApiResponse(responseCode = "401", description = "JWT ausente o inválido", content = @Content),
                    @ApiResponse(responseCode = "403", description = "El JWT no tiene ROLE_ADMIN", content = @Content)
            }
    )
    @GetMapping("/alerts")
    public ResponseEntity<Page<AlertEntry>> getAlerts(
            @Parameter(in = ParameterIn.QUERY, description = "Filtra alertas de un usuario concreto", example = "javi")
            @RequestParam(required = false) String username,

            @Parameter(in = ParameterIn.QUERY, description = "Número de página (0-based)", example = "0")
            @RequestParam(defaultValue = "0") int page,

            @Parameter(in = ParameterIn.QUERY, description = "Registros por página (máx. 100)", example = "20")
            @RequestParam(defaultValue = "20") int size) {

        Pageable pageable = PageRequest.of(page, Math.min(size, 100));

        Page<AlertEntry> result = (username != null)
                ? alertEntryRepository.findByUsernameOrderByDetectedAtDesc(username, pageable)
                : alertEntryRepository.findAllByOrderByDetectedAtDesc(pageable);

        return ResponseEntity.ok(result);
    }
}
