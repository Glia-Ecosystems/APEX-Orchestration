package com.netflix.conductor.contribs.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourcesLoader;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(KafkaProperties.class)
@ConditionalOnProperty(name = "conductor.kafka.additional.component", havingValue = "true")
public class KafkaConfiguration {

    @ConditionalOnMissingBean
    @Bean
    public ResourceHandler resourceHandler(final ObjectMapper objectMapper){
        return new ResourceHandler(objectMapper, resourcesLoader());
    }

    public ResourcesLoader resourcesLoader(){
        final String RESOURCE_PATH = "com.netflix.conductor.rest.controllers";
        return new ResourcesLoader(RESOURCE_PATH);
    }

}
