package com.cyurtoz.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.util.Collections;

@Configuration
@EnableSwagger2
public class SwaggerConfig {

    @Value("${info.build.version}")
    private String version;

    @Value("${kafka-playback-service.source.bootstrap-servers}")
    private String sourceHost;

    @Value("${kafka-playback-service.target.bootstrap-servers}")
    private String targetHost;

    @Bean
    public Docket api() {
        return new Docket(DocumentationType.SWAGGER_2)
                .select()
                .apis(RequestHandlerSelectors.basePackage("com.cyurtoz"))
                .paths(PathSelectors.any())
                .build()
                .apiInfo(apiInfo());
    }

    private ApiInfo apiInfo() {
        return new ApiInfo(
                "Kafka Playback Service",
                String.format("<h3>Source Kafka : %s </h3> <h3>Target Kafka : %s</h3>", sourceHost, targetHost),
                version,
                "", null, "", "", Collections.emptyList());
    }

}
