package com.netflix.conductor.contribs.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourcesLoader;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(KafkaProperties.class)
@ConditionalOnProperty(name = "conductor.kafka.additional.component", havingValue = "true")
public class KafkaConfiguration {

    @ConditionalOnMissingBean
    @Bean
    public ResourceHandler resourceHandler(final ObjectMapper objectMapper, final ApplicationContext applicationContext){
        return new ResourceHandler(applicationContext, objectMapper, resourcesLoader(applicationContext));
    }

    public ResourcesLoader resourcesLoader(final ApplicationContext applicationContext){
        final String RESOURCE_PATH = "com.netflix.conductor.rest.controllers";
        return new ResourcesLoader(applicationContext, RESOURCE_PATH);
    }

}
