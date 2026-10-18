-- ============================================================
-- ko2-kafka-events — DDL manual para Aiven MySQL
-- Ejecutar UNA VEZ antes del primer despliegue
-- ============================================================

-- Tabla de auditoría: registra cada evento del ecosistema KO2
--
-- 2026-10-18: dos columnas estaban mal escritas aquí. Este fichero decía
-- client_ip y metadata, pero AuditEntry las mapea como ip_address y detail, y
-- la aplicación va con ddl-auto=none: quien creara la tabla con el DDL de
-- antes se encontraría al consumer fallando en cada save() sin hacer ack, es
-- decir, reintentando el mismo evento para siempre. La tabla real de Aiven sí
-- coincide con la entidad (comprobado el 18/10 viendo que el consumer graba
-- sin error en producción); era este fichero el que estaba desfasado.
CREATE TABLE IF NOT EXISTS audit_events (
    id         BIGINT       NOT NULL AUTO_INCREMENT,
    event_type VARCHAR(30)  NOT NULL,
    username   VARCHAR(100) NOT NULL,
    ip_address VARCHAR(50),
    timestamp  DATETIME     NOT NULL,
    detail     TEXT,

    PRIMARY KEY (id),
    INDEX idx_audit_username  (username),
    INDEX idx_audit_timestamp (timestamp),
    INDEX idx_audit_event_type (event_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- Tabla de alertas: usuario con >10 requests en 1 minuto
CREATE TABLE IF NOT EXISTS alert_events (
    id            BIGINT       NOT NULL AUTO_INCREMENT,
    username      VARCHAR(100) NOT NULL,
    request_count INT          NOT NULL,
    window_start  DATETIME     NOT NULL,
    detected_at   DATETIME     NOT NULL,

    PRIMARY KEY (id),
    INDEX idx_alert_username    (username),
    INDEX idx_alert_detected_at (detected_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
