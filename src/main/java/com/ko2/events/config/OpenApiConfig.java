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
                        Audit and alerting microservice for the KO2 Currency & Weather Hub.

                        It consumes three Kafka topics (`user.login`, `currency.query`, `weather.query`) with two independent consumer groups and persists them to MySQL: one writes the audit trail, the other watches for rate-limit abuse. Nothing on the request path waits for any of it, and the consumer replays from its last committed offset after a restart.

                        **Every endpoint below needs a ROLE_ADMIN JWT.** Four steps:

                        1. In the **definition selector at the top right**, switch to **Auth Service**.
                        2. Run `POST /auth/login` with `{ "username": "admin", "password": "admin123" }`. The `user` account authenticates fine but gets 403 here: these endpoints are admin-only.
                        3. Copy the `token` value out of the response.
                        4. Come back to this definition, click **Authorize**, and paste the token *without* the `Bearer` prefix.
                        """,
                contact = @Contact(name = "ko2javier", url = "https://github.com/ko2javier")
        ),
        servers = @Server(url = "https://api.ko2-oreilly.com", description = "Production (via Gateway)")
)
@SecurityScheme(
        name = "bearerAuth",
        type = SecuritySchemeType.HTTP,
        scheme = "bearer",
        bearerFormat = "JWT",
        in = SecuritySchemeIn.HEADER,
        description = "Paste the ROLE_ADMIN JWT from POST /auth/login here, without the 'Bearer ' prefix."
)
public class OpenApiConfig {
}
