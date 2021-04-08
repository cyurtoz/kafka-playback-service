package com.cyurtoz.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class TargetKafkaService {

    private final String targetKafkaHost;
    private final DefaultKafkaProducerFactory<String, Object> factory;
    private final ConcurrentHashMap<String, Producer<String, Object>> topicToProducerMap;

    public TargetKafkaService(@Value("${kafka-playback-service.target.bootstrap-servers}") String targetKafkaHost) {
        this.targetKafkaHost = targetKafkaHost;
        this.factory = createProducerFactory();
        this.topicToProducerMap = new ConcurrentHashMap<>();
    }

    public void send(String topic, Object message) {
        Producer<String, Object> producer = topicToProducerMap.getOrDefault(topic, createAndSaveProducer(topic));
        producer.send(new ProducerRecord<>(topic, message));
        log.info("DEST {} - {}", topic, message);
    }

    private Producer<String, Object> createAndSaveProducer(String topic) {
        if (topicToProducerMap.containsKey(topic)) return topicToProducerMap.get(topic);
        else return factory.createProducer();
    }

    public void stopForTopic(String topic) {
        if (topicToProducerMap.containsKey(topic)) {
            topicToProducerMap.get(topic).close();
            topicToProducerMap.remove(topic);
        }
    }

    private DefaultKafkaProducerFactory<String, Object> createProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, targetKafkaHost);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }
}
