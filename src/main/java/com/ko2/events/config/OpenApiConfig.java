package com.ko2.events.config;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeIn;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import io.swagger.v3.oas.annotations.servers.Server;
import org.springframework.context.annotation.Configuration;

@Configuration
@OpenAPIDefinition(
        info = @Info(
                title = "KO2 Kafka Events API",
                version = "1.0",
                description = """
                        Microservicio de auditoría y alertas del ecosistema KO2 Currency & Weather Hub.

                        Consume eventos Kafka de tres topics (`user.login`, `currency.query`, `weather.query`) \
                        y los persiste en MySQL (Aiven). Expone endpoints REST para consultar el histórico \
                        de auditoría y las alertas de rate-limiting.

                        **Autenticación:** todos los endpoints requieren un JWT de ROLE_ADMIN. \
                        Obtén el token en el Auth Service (`POST /auth/login`) y pégalo en el botón Authorize.
                        """,
                contact = @Contact(name = "ko2javier", url = "https://github.com/ko2javier")
        ),
        servers = @Server(url = "http://167.235.77.17:6000", description = "Hetzner VPS")
)
@SecurityScheme(
        name = "bearerAuth",
        type = SecuritySchemeType.HTTP,
        scheme = "bearer",
        bearerFormat = "JWT",
        in = SecuritySchemeIn.HEADER,
        description = "Introduce el JWT obtenido en POST /auth/login (sin el prefijo 'Bearer ')"
)
public class OpenApiConfig {
}
