# ── Stage 1: Build ──────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jdk-alpine AS builder

WORKDIR /app

# Copiar archivos de build primero (aprovecha cache de capas Docker)
COPY gradlew .
COPY gradle gradle
COPY build.gradle .
COPY settings.gradle .

# Descargar dependencias (cacheado si build.gradle no cambia)
RUN chmod +x gradlew && ./gradlew dependencies --no-daemon -q

# Copiar fuentes y construir el fat JAR
COPY src src
RUN ./gradlew bootJar --no-daemon -q

# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

# Usuario no-root por seguridad
RUN addgroup -S ko2group && adduser -S ko2user -G ko2group
USER ko2user

COPY --from=builder /app/build/libs/*.jar app.jar

EXPOSE 6000
EXPOSE 6001

# JVM tuning para contenedor (respeta cgroup limits)
ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "-Djava.security.egd=file:/dev/./urandom", \
  "-jar", "app.jar"]
