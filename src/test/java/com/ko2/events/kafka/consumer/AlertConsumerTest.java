package com.ko2.events.kafka.consumer;

import com.ko2.events.kafka.event.KafkaEvent;
import com.ko2.events.model.AlertEntry;
import com.ko2.events.repository.AlertEntryRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * La ventana deslizante de AlertConsumer, sin contenedores.
 *
 * <p>Va con dobles a proposito: el estado que decide si hay alerta esta en un
 * ConcurrentHashMap dentro del propio consumer, no en Kafka ni en la base, y
 * eso se prueba llamando al metodo con los instantes que interesan. Levantar
 * un broker para esto solo aniadiria veinte segundos y la posibilidad de un
 * test intermitente.
 *
 * <p>Lo que fija: el umbral es <b>estrictamente mayor</b> que 10, el conteo
 * usa el timestamp DEL EVENTO y no la hora de la maquina, y un evento sin
 * username se descarta con ack (si no se hiciera ack, Kafka lo reintentaria
 * para siempre y el grupo se quedaria atascado).
 */
class AlertConsumerTest {

    private static final Instant T0 = Instant.parse("2026-01-01T10:00:00Z");

    private AlertEntryRepository repositorio;
    private Acknowledgment ack;
    private AlertConsumer consumer;

    @BeforeEach
    void preparar() {
        repositorio = mock(AlertEntryRepository.class);
        ack = mock(Acknowledgment.class);
        when(repositorio.save(any(AlertEntry.class))).thenAnswer(i -> i.getArgument(0));
        consumer = new AlertConsumer(repositorio);
    }

    @Test
    @DisplayName("diez eventos en la ventana no generan alerta: el umbral es > 10")
    void dieznoDisparan() {
        for (int i = 0; i < 10; i++) {
            consumir("ana", T0.plusSeconds(i));
        }

        verify(repositorio, never()).save(any());
        // Los diez se confirman igual: nada que reintentar.
        verify(ack, times(10)).acknowledge();
    }

    @Test
    @DisplayName("el undecimo evento dispara la alerta, con el conteo de la ventana")
    void elUndecimoDispara() {
        for (int i = 0; i < 11; i++) {
            consumir("ana", T0.plusSeconds(i));
        }

        ArgumentCaptor<AlertEntry> capturada = ArgumentCaptor.forClass(AlertEntry.class);
        verify(repositorio, times(1)).save(capturada.capture());

        AlertEntry alerta = capturada.getValue();
        assertThat(alerta.getUsername()).isEqualTo("ana");
        assertThat(alerta.getRequestCount()).isEqualTo(11);
        // detectedAt es la hora del evento, en UTC; windowStart, un minuto antes.
        assertThat(alerta.getDetectedAt()).isEqualTo("2026-01-01T10:00:10");
        assertThat(alerta.getWindowStart()).isEqualTo("2026-01-01T09:59:10");
    }

    @Test
    @DisplayName("la ventana es por usuario: diez de cada uno no disparan nada")
    void laVentanaEsPorUsuario() {
        for (int i = 0; i < 10; i++) {
            consumir("ana", T0.plusSeconds(i));
            consumir("bea", T0.plusSeconds(i));
        }

        verify(repositorio, never()).save(any());
    }

    @Test
    @DisplayName("los eventos que salen de la ventana de 60 s se descartan del conteo")
    void laVentanaSeVacia() {
        // Diez eventos, y el siguiente mas de un minuto despues: al llegar el
        // undecimo, los diez primeros ya han caido fuera, asi que el conteo es
        // 1 y no 11.
        for (int i = 0; i < 10; i++) {
            consumir("ana", T0.plusSeconds(i));
        }
        consumir("ana", T0.plusSeconds(120));

        verify(repositorio, never()).save(any());
    }

    @Test
    @DisplayName("un evento nulo o sin username se confirma y no se cuenta")
    void eventosQueSeDescartan() {
        consumer.consume(new ConsumerRecord<>("user.login", 0, 0L, "k", null), ack);
        consumer.consume(new ConsumerRecord<>("user.login", 0, 1L, "k",
                KafkaEvent.builder().eventType("USER_LOGIN").username(null).build()), ack);

        verify(repositorio, never()).save(any());
        verify(ack, times(2)).acknowledge();
    }

    private void consumir(String usuario, Instant cuando) {
        KafkaEvent evento = KafkaEvent.builder()
                .eventType("CURRENCY_QUERY")
                .username(usuario)
                .clientIp("10.0.0.1")
                .timestamp(cuando)
                .metadata(Map.of())
                .build();
        consumer.consume(new ConsumerRecord<>("currency.query", 0, 0L, usuario, evento), ack);
    }
}
