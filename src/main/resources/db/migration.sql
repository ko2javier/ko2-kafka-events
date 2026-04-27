-- ============================================================
-- ko2-kafka-events — DDL manual para Aiven MySQL
-- Ejecutar UNA VEZ antes del primer despliegue
-- ============================================================

-- Tabla de auditoría: registra cada evento del ecosistema KO2
CREATE TABLE IF NOT EXISTS audit_events (
    id         BIGINT       NOT NULL AUTO_INCREMENT,
    event_type VARCHAR(30)  NOT NULL,
    username   VARCHAR(100) NOT NULL,
    client_ip  VARCHAR(50),
    timestamp  DATETIME     NOT NULL,
    metadata   TEXT,

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
