package com.ko2.events.support;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Base de los tests de integracion de ko2-kafka-events. Levanta lo que la
 * aplicacion necesita de verdad y rellena las cuatro variables que
 * application.yml pide sin valor por defecto (DB_URL, DB_USERNAME,
 * DB_PASSWORD, KAFKA_BOOTSTRAP_SERVERS, JWT_SECRET).
 *
 * <p>Hasta el 2026-10-18 este repo no tenia NI UN TEST, y el Dockerfile
 * construye con {@code bootJar}, que no ejecuta la tarea test: desplegaba a
 * produccion sin ejecutar nada.
 *
 * <p><b>Aqui Kafka SI se levanta</b>, al contrario que en auth-currency-data-hub
 * y currency-data-hub. Aquellos solo producen eventos; este es el CONSUMIDOR
 * del ecosistema (dos @KafkaListener, grupos audit-group y alert-group), asi
 * que sin broker lo unico que se podria probar son los endpoints REST, y se
 * quedaria fuera justo lo que hace este servicio.
 *
 * <p><b>Por que un MySQL de verdad y no un H2:</b> las dos entidades se
 * consultan con Pageable y orden descendente, y el contrato de los endpoints es
 * esa pagina. Con un motor distinto al de produccion se estaria comprobando
 * otro SQL.
 *
 * <p><b>La imagen del broker es la de Confluent, no apache/kafka:3.7.0 como en
 * produccion:</b> el KafkaContainer de Testcontainers 1.19.8 (la version que
 * fija el BOM de Spring Boot 3.3.5) solo sabe arrancar la de Confluent. Es el
 * mismo protocolo y la misma version de Kafka; lo que se prueba aqui es la
 * configuracion de serializadores, grupos y ack de este servicio, no el
 * broker.
 *
 * <p><b>ddl-auto en los tests es create-drop</b>, mientras en produccion es
 * none y el esquema se crea a mano. Hibernate crea audit_events y alert_events
 * a partir de las entidades. Ojo: {@code src/main/resources/db/migration.sql}
 * NO coincide con las entidades (llama client_ip y metadata a lo que la
 * entidad llama ip_address y detail), asi que no sirve como script de
 * arranque; la tabla real de Aiven si coincide con la entidad — se comprobo
 * el 2026-10-18 viendo que el consumer graba sin error en produccion.
 *
 * <p><b>Los contenedores arrancan en un bloque static y no con
 * {@code @Container}</b>: con la anotacion los arranca la extension de
 * Testcontainers, y su orden frente a la de Spring no esta garantizado. Si
 * Spring crea el contexto primero, {@link DynamicPropertySource} lee un puerto
 * que todavia no existe. Los para Ryuk al morir la JVM. Al ser static y estar
 * en la clase base, los dos contenedores se arrancan UNA vez para toda la
 * bateria y las clases hijas comparten el mismo contexto de Spring.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class EventsIntegrationTest {

    /**
     * 32 bytes como minimo: JwtUtil usa Keys.hmacShaKeyFor, que exige 256 bits
     * para HS256 y con menos lanza WeakKeyException al construir la clave. No
     * es el de produccion.
     */
    protected static final String SECRETO = "secreto-de-test-con-mas-de-32-bytes!!";

    private static final MySQLContainer<?> MYSQL =
            new MySQLContainer<>(DockerImageName.parse("mysql:8.0"));

    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    static {
        MYSQL.start();
        KAFKA.start();
    }

    @DynamicPropertySource
    static void propiedades(DynamicPropertyRegistry registro) {
        registro.add("DB_URL", MYSQL::getJdbcUrl);
        registro.add("DB_USERNAME", MYSQL::getUsername);
        registro.add("DB_PASSWORD", MYSQL::getPassword);
        registro.add("KAFKA_BOOTSTRAP_SERVERS", KAFKA::getBootstrapServers);
        registro.add("JWT_SECRET", () -> SECRETO);
        // Produccion va con none y el esquema a mano; ver el comentario de la clase.
        registro.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
        // application.yml fija el actuator en el 6001. En los tests eso serian
        // dos baterias peleandose por un puerto fijo: 0 -> puerto libre, que se
        // inyecta con @LocalManagementPort.
        registro.add("management.server.port", () -> "0");
    }
}
