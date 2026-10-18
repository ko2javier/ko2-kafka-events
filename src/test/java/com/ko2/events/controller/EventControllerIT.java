package com.ko2.events.controller;

import com.ko2.events.model.AlertEntry;
import com.ko2.events.model.AuditEntry;
import com.ko2.events.repository.AlertEntryRepository;
import com.ko2.events.repository.AuditEntryRepository;
import com.ko2.events.support.EventsIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Los dos endpoints de consulta: GET /events/audit y GET /events/alerts.
 *
 * <p>La autenticacion de este servicio NO es un JWT: JwtAuthenticationFilter
 * lee las cabeceras X-User-Name y X-User-Roles que le inyecta el gateway tras
 * validar el token. Por eso los tests mandan cabeceras y no un Bearer. Ver el
 * test marcado como CARACTERIZACION al final: eso tiene consecuencias.
 */
class EventControllerIT extends EventsIntegrationTest {

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private AuditEntryRepository auditEntryRepository;

    @Autowired
    private AlertEntryRepository alertEntryRepository;

    @LocalManagementPort
    private int puertoActuator;

    /**
     * Las dos tablas se vacian antes de cada test. Hace falta porque
     * AuditConsumerIT publica eventos de verdad en Kafka y el consumer los
     * graba en la misma base: sin esto, un test de esta clase contaria filas
     * que no ha creado el.
     */
    @BeforeEach
    void limpiar() {
        auditEntryRepository.deleteAll();
        alertEntryRepository.deleteAll();
    }

    // ── Seguridad ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("sin cabeceras de usuario, /events/audit no se sirve")
    void auditSinAutenticacion() {
        ResponseEntity<String> respuesta = rest.getForEntity("/events/audit", String.class);

        // 403 y no 401, aunque el Swagger de EventController anuncie un 401:
        // SecurityConfig no declara ningun mecanismo de autenticacion
        // (ni httpBasic ni formLogin), asi que Spring Security se queda con su
        // entry point por defecto, Http403ForbiddenEntryPoint. El 401 del
        // Swagger es el que devuelve el GATEWAY cuando falta el token, no este
        // servicio.
        assertThat(respuesta.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
    }

    @Test
    @DisplayName("con un rol que no es ADMIN, /events/audit devuelve 403")
    void auditConRolQueNoEsAdmin() {
        ResponseEntity<String> respuesta = get("/events/audit", "javi", "ROLE_USER");

        assertThat(respuesta.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
    }

    @Test
    @DisplayName("lo mismo en /events/alerts: ROLE_USER no entra")
    void alertsConRolQueNoEsAdmin() {
        ResponseEntity<String> respuesta = get("/events/alerts", "javi", "ROLE_USER");

        assertThat(respuesta.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
    }

    @Test
    @DisplayName("el actuator es publico: responde sin cabeceras de usuario")
    void actuatorAbierto() {
        // Con su propio puerto, como en produccion (alli el 6001), que es el que
        // raspa Prometheus. SecurityConfig lo deja en permitAll a proposito.
        ResponseEntity<String> respuesta = new TestRestTemplate().getForEntity(
                "http://localhost:" + puertoActuator + "/actuator/health", String.class);

        assertThat(respuesta.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(respuesta.getBody()).contains("\"status\":\"UP\"");
    }

    // ── /events/audit ────────────────────────────────────────────────────────

    @Test
    @DisplayName("sin filtros devuelve todo, ordenado por timestamp descendente")
    void auditSinFiltros() {
        guardarAudit("USER_LOGIN", "ana", LocalDateTime.of(2026, 1, 1, 10, 0));
        guardarAudit("CURRENCY_QUERY", "bea", LocalDateTime.of(2026, 1, 3, 10, 0));
        guardarAudit("WEATHER_QUERY", "ana", LocalDateTime.of(2026, 1, 2, 10, 0));

        ResponseEntity<String> respuesta = get("/events/audit", "admin", "ROLE_ADMIN");

        assertThat(respuesta.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(respuesta.getBody()).contains("\"totalElements\":3");
        // El mas nuevo primero: bea (dia 3), ana (dia 2), ana (dia 1).
        assertThat(ordenDeUsernames(respuesta.getBody())).containsExactly("bea", "ana", "ana");
    }

    @Test
    @DisplayName("filtro por username")
    void auditPorUsername() {
        guardarAudit("USER_LOGIN", "ana", LocalDateTime.of(2026, 1, 1, 10, 0));
        guardarAudit("USER_LOGIN", "bea", LocalDateTime.of(2026, 1, 2, 10, 0));

        ResponseEntity<String> respuesta = get("/events/audit?username=ana", "admin", "ROLE_ADMIN");

        assertThat(respuesta.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(respuesta.getBody()).contains("\"totalElements\":1");
        assertThat(ordenDeUsernames(respuesta.getBody())).containsExactly("ana");
    }

    @Test
    @DisplayName("filtro por eventType")
    void auditPorEventType() {
        guardarAudit("USER_LOGIN", "ana", LocalDateTime.of(2026, 1, 1, 10, 0));
        guardarAudit("WEATHER_QUERY", "bea", LocalDateTime.of(2026, 1, 2, 10, 0));

        ResponseEntity<String> respuesta = get("/events/audit?eventType=WEATHER_QUERY", "admin", "ROLE_ADMIN");

        assertThat(respuesta.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(respuesta.getBody()).contains("\"totalElements\":1");
        assertThat(ordenDeUsernames(respuesta.getBody())).containsExactly("bea");
    }

    @Test
    @DisplayName("filtro por rango de fechas: los extremos entran (BETWEEN es inclusivo)")
    void auditPorRangoDeFechas() {
        guardarAudit("USER_LOGIN", "fuera-antes", LocalDateTime.of(2026, 1, 1, 9, 59, 59));
        guardarAudit("USER_LOGIN", "borde-inicio", LocalDateTime.of(2026, 1, 1, 10, 0, 0));
        guardarAudit("USER_LOGIN", "dentro", LocalDateTime.of(2026, 1, 1, 11, 0, 0));
        guardarAudit("USER_LOGIN", "borde-fin", LocalDateTime.of(2026, 1, 1, 12, 0, 0));
        guardarAudit("USER_LOGIN", "fuera-despues", LocalDateTime.of(2026, 1, 1, 12, 0, 1));

        ResponseEntity<String> respuesta = get(
                "/events/audit?from=2026-01-01T10:00:00&to=2026-01-01T12:00:00", "admin", "ROLE_ADMIN");

        assertThat(respuesta.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(ordenDeUsernames(respuesta.getBody()))
                .containsExactly("borde-fin", "dentro", "borde-inicio");
    }

    @Test
    @DisplayName("CONTRATO: el rango de fechas gana sobre username, no se combinan")
    void auditRangoYUsernameNoSeCombinan() {
        // Los cuatro filtros del endpoint son MUTUAMENTE EXCLUYENTES: el
        // controlador aplica el primero que coincida, y from+to es el primero.
        // Quien llame con los dos esperando una interseccion recibe otra cosa.
        guardarAudit("USER_LOGIN", "ana", LocalDateTime.of(2026, 1, 1, 11, 0));
        guardarAudit("USER_LOGIN", "bea", LocalDateTime.of(2026, 1, 1, 11, 30));

        ResponseEntity<String> respuesta = get(
                "/events/audit?username=ana&from=2026-01-01T10:00:00&to=2026-01-01T12:00:00",
                "admin", "ROLE_ADMIN");

        assertThat(respuesta.getStatusCode()).isEqualTo(HttpStatus.OK);
        // Devuelve las dos, no solo la de ana.
        assertThat(ordenDeUsernames(respuesta.getBody())).containsExactly("bea", "ana");
    }

    @Test
    @DisplayName("size se recorta a 100 aunque se pidan mas")
    void auditSizeRecortado() {
        guardarAudit("USER_LOGIN", "ana", LocalDateTime.of(2026, 1, 1, 10, 0));

        ResponseEntity<String> respuesta = get("/events/audit?size=5000", "admin", "ROLE_ADMIN");

        assertThat(respuesta.getStatusCode()).isEqualTo(HttpStatus.OK);
        // Math.min(size, 100) en el controlador. Sin ese tope, un size enorme
        // se traeria la tabla entera de auditoria en una sola respuesta.
        assertThat(respuesta.getBody()).contains("\"size\":100");
    }

    // ── /events/alerts ───────────────────────────────────────────────────────

    @Test
    @DisplayName("alertas sin filtro, ordenadas por detectedAt descendente")
    void alertsSinFiltro() {
        guardarAlerta("ana", 11, LocalDateTime.of(2026, 1, 1, 10, 0));
        guardarAlerta("bea", 15, LocalDateTime.of(2026, 1, 2, 10, 0));

        ResponseEntity<String> respuesta = get("/events/alerts", "admin", "ROLE_ADMIN");

        assertThat(respuesta.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(respuesta.getBody()).contains("\"totalElements\":2");
        assertThat(ordenDeUsernames(respuesta.getBody())).containsExactly("bea", "ana");
    }

    @Test
    @DisplayName("alertas filtradas por username")
    void alertsPorUsername() {
        guardarAlerta("ana", 11, LocalDateTime.of(2026, 1, 1, 10, 0));
        guardarAlerta("bea", 15, LocalDateTime.of(2026, 1, 2, 10, 0));

        ResponseEntity<String> respuesta = get("/events/alerts?username=ana", "admin", "ROLE_ADMIN");

        assertThat(respuesta.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(respuesta.getBody()).contains("\"totalElements\":1");
        assertThat(ordenDeUsernames(respuesta.getBody())).containsExactly("ana");
    }

    // ── Caracterizacion ──────────────────────────────────────────────────────

    @Test
    @DisplayName("CARACTERIZACION: dos cabeceras inventadas bastan para entrar como ADMIN")
    void sinTokenPeroConCabecerasSeEntraComoAdmin() {
        // Este test describe el comportamiento de HOY, no el deseado.
        //
        // JwtAuthenticationFilter se fia de X-User-Name y X-User-Roles sin
        // validar nada, y JwtUtil -- la clase que SI valida el token -- no se
        // usa en ningun sitio del proyecto (grep, 2026-10-18). El diseno
        // asume que al 6000 solo llega el gateway.
        //
        // Mientras los puertos 6000/6001 se publiquen en todas las interfaces
        // del VPS, cualquiera puede leer la auditoria entera mandando estas
        // dos cabeceras. Esta apuntado como pendiente grave.
        //
        // Si algun dia este servicio valida el JWT, este test se pondra ROJO a
        // proposito: hay que cambiarlo en el mismo commit.
        guardarAudit("USER_LOGIN", "ana", LocalDateTime.of(2026, 1, 1, 10, 0));

        ResponseEntity<String> respuesta = get("/events/audit", "cualquiera", "ROLE_ADMIN");

        assertThat(respuesta.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(respuesta.getBody()).contains("ana");
    }

    // ── Apoyo ────────────────────────────────────────────────────────────────

    private ResponseEntity<String> get(String ruta, String usuario, String roles) {
        HttpHeaders cabeceras = new HttpHeaders();
        cabeceras.set("X-User-Name", usuario);
        cabeceras.set("X-User-Roles", roles);
        return rest.exchange(ruta, HttpMethod.GET, new HttpEntity<>(cabeceras), String.class);
    }

    private void guardarAudit(String tipo, String usuario, LocalDateTime cuando) {
        auditEntryRepository.save(AuditEntry.builder()
                .eventType(tipo)
                .username(usuario)
                .ipAddress("10.0.0.1")
                .timestamp(cuando)
                .detail("{}")
                .build());
    }

    private void guardarAlerta(String usuario, int cuantos, LocalDateTime cuando) {
        alertEntryRepository.save(AlertEntry.builder()
                .username(usuario)
                .requestCount(cuantos)
                .windowStart(cuando.minusMinutes(1))
                .detectedAt(cuando)
                .build());
    }

    /**
     * Los usernames del JSON en el orden en que vienen. Se lee el cuerpo como
     * texto y no como Page<AuditEntry> porque Jackson no sabe reconstruir un
     * PageImpl sin un deserializador a medida, y lo que se esta comprobando es
     * el orden, que el texto ya lo dice.
     */
    private static java.util.List<String> ordenDeUsernames(String cuerpo) {
        java.util.regex.Matcher m =
                java.util.regex.Pattern.compile("\"username\":\"([^\"]+)\"").matcher(cuerpo);
        java.util.List<String> encontrados = new java.util.ArrayList<>();
        while (m.find()) {
            encontrados.add(m.group(1));
        }
        return encontrados;
    }
}
