package com.netflix.conductor.contribs.kafka.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;


@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(KafkaProperties.class)
@ConditionalOnProperty(name = "conductor.kafka.additional.component", havingValue = "true")
public class KafkaConfiguration {

}
