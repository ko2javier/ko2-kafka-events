package com.ko2.events.kafka.consumer;

import com.ko2.events.kafka.event.KafkaEvent;
import com.ko2.events.model.AuditEntry;
import com.ko2.events.repository.AuditEntryRepository;
import com.ko2.events.support.EventsIntegrationTest;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * El camino completo del servicio: un evento publicado en Kafka acaba siendo
 * una fila de audit_events.
 *
 * <p>Esto es lo que no se podia probar sin broker, y es justo lo que hace este
 * microservicio. Pasa por la cadena entera de produccion: JsonSerializer sin
 * cabeceras de tipo -> el topic de verdad -> ErrorHandlingDeserializer +
 * JsonDeserializer con spring.json.value.default.type -> AuditConsumer ->
 * JPA -> MySQL. Si se rompe cualquiera de esas piezas (por ejemplo, si alguien
 * quita VALUE_DEFAULT_TYPE de KafkaConfig), estos tests se caen.
 *
 * <p>Se publica con el KafkaTemplate de la aplicacion, el que configura
 * KafkaConfig, y no con uno de test: con otro serializador se estaria probando
 * un camino que en produccion no existe.
 *
 * <p>Cada test usa un username unico. Los dos consumers comparten topics y la
 * tabla la comparten con EventControllerIT, asi que buscar por username es la
 * unica forma de que un test no vea las filas de otro.
 */
class AuditConsumerIT extends EventsIntegrationTest {

    @Autowired
    private KafkaTemplate<String, KafkaEvent> kafkaTemplate;

    @Autowired
    private AuditEntryRepository auditEntryRepository;

    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    @DisplayName("un evento de user.login se persiste con todos sus campos")
    void loginSePersiste() {
        String usuario = "login-" + UUID.randomUUID();
        Instant cuando = Instant.parse("2026-01-01T10:00:00Z");

        kafkaTemplate.send("user.login", usuario, KafkaEvent.builder()
                .eventType("USER_LOGIN")
                .username(usuario)
                .clientIp("139.47.49.16")
                .timestamp(cuando)
                .metadata(Map.of())
                .build());

        AuditEntry fila = esperarFilaDe(usuario);

        assertThat(fila.getEventType()).isEqualTo("USER_LOGIN");
        assertThat(fila.getIpAddress()).isEqualTo("139.47.49.16");
        // El consumer convierte el Instant a LocalDateTime en UTC, no en la
        // zona del servidor.
        assertThat(fila.getTimestamp()).isEqualTo("2026-01-01T10:00");
        // metadata vacio -> "{}" y no null: la columna es TEXT y el contrato
        // de la API dice que detail siempre es un JSON valido.
        assertThat(fila.getDetail()).isEqualTo("{}");
    }

    @Test
    @DisplayName("la metadata de currency.query se guarda serializada como JSON")
    void metadataSeSerializa() {
        String usuario = "currency-" + UUID.randomUUID();

        kafkaTemplate.send("currency.query", usuario, KafkaEvent.builder()
                .eventType("CURRENCY_QUERY")
                .username(usuario)
                .clientIp("10.0.0.9")
                .timestamp(Instant.parse("2026-01-02T08:30:00Z"))
                .metadata(Map.of("from", "USD", "to", "EUR"))
                .build());

        AuditEntry fila = esperarFilaDe(usuario);

        assertThat(fila.getEventType()).isEqualTo("CURRENCY_QUERY");
        assertThat(fila.getDetail()).contains("\"from\":\"USD\"", "\"to\":\"EUR\"");
    }

    @Test
    @DisplayName("el tercer topic, weather.query, lo consume el mismo listener")
    void weatherSePersiste() {
        String usuario = "weather-" + UUID.randomUUID();

        kafkaTemplate.send("weather.query", usuario, KafkaEvent.builder()
                .eventType("WEATHER_QUERY")
                .username(usuario)
                .clientIp("10.0.0.9")
                .timestamp(Instant.parse("2026-01-03T08:30:00Z"))
                .metadata(Map.of("city", "Madrid"))
                .build());

        AuditEntry fila = esperarFilaDe(usuario);

        assertThat(fila.getEventType()).isEqualTo("WEATHER_QUERY");
        assertThat(fila.getDetail()).contains("\"city\":\"Madrid\"");
    }

    @Test
    @DisplayName("un evento sin metadata tambien se guarda, con detail {}")
    void sinMetadata() {
        String usuario = "sin-metadata-" + UUID.randomUUID();

        kafkaTemplate.send("user.login", usuario, KafkaEvent.builder()
                .eventType("USER_LOGIN")
                .username(usuario)
                .clientIp(null)
                .timestamp(Instant.parse("2026-01-04T08:30:00Z"))
                .metadata(null)
                .build());

        AuditEntry fila = esperarFilaDe(usuario);

        assertThat(fila.getDetail()).isEqualTo("{}");
        // ip_address si admite null: la columna no lleva nullable = false.
        assertThat(fila.getIpAddress()).isNull();
    }

    @Test
    @DisplayName("el consumer publica consumer lag en Micrometer (lo raspa Prometheus)")
    void publicaConsumerLag() {
        String usuario = "lag-" + UUID.randomUUID();

        kafkaTemplate.send("user.login", usuario, KafkaEvent.builder()
                .eventType("USER_LOGIN")
                .username(usuario)
                .clientIp("10.0.0.9")
                .timestamp(Instant.parse("2026-01-05T08:30:00Z"))
                .metadata(Map.of())
                .build());
        esperarFilaDe(usuario);

        // kafka.consumer.* son las metricas del CLIENTE de Kafka, y solo
        // existen porque KafkaConfig engancha a mano un
        // MicrometerConsumerListener: Spring Boot se lo pone unicamente a la
        // fabrica que autoconfigura el. Si alguien quita esa linea, Prometheus
        // deja de ver si un consumidor se queda atras, y en el panel no se
        // nota: lo que desaparece es una serie, no un dato en rojo.
        // El nombre exacto importa, y no es el que uno supone: la serie por
        // particion (records.lag) solo aparece cuando hay particiones con
        // retraso asignadas, mientras que records.lag.max existe en cuanto el
        // cliente hace el primer fetch. Es la que sirve para una alerta, y es
        // la que se comprobo en el registro de verdad (2026-10-18).
        assertThat(meterRegistry.find("kafka.consumer.fetch.manager.records.lag.max").meters())
                .as("kafka_consumer_fetch_manager_records_lag_max")
                .isNotEmpty();
        // Y del lado del productor, su gemela.
        assertThat(meterRegistry.find("kafka.producer.record.send.total").meters())
                .as("kafka_producer_record_send_total")
                .isNotEmpty();
    }

    /**
     * El consumer trabaja en su propio hilo, asi que la fila no esta cuando
     * termina el send. 20 s de margen porque el primer evento de la bateria
     * paga tambien la asignacion de particiones del grupo.
     */
    private AuditEntry esperarFilaDe(String usuario) {
        await().atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> !filasDe(usuario).isEmpty());

        List<AuditEntry> filas = filasDe(usuario);
        assertThat(filas).hasSize(1);
        return filas.get(0);
    }

    private List<AuditEntry> filasDe(String usuario) {
        return auditEntryRepository.findAll().stream()
                .filter(f -> usuario.equals(f.getUsername()))
                .toList();
    }
}
