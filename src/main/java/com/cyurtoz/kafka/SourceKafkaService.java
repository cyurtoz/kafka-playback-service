package com.cyurtoz.kafka;

import com.cyurtoz.model.KafkaMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class SourceKafkaService {

    private final String sourceKafkaHost;
    private final ObjectMapper mapper;
    private static final long INFINITE_OFFSET = -1L;

    public SourceKafkaService(@Value("${kafka-playback-service.source.bootstrap-servers}") String sourceKafkaHost) {
        this.sourceKafkaHost = sourceKafkaHost;
        this.mapper = new ObjectMapper();
    }

    public <T> List<KafkaMessage<T>> findMessagesBetween(String sourceTopic, Date startDate, Date endDate, Class<T> type) {
        var sourceKafkaConsumer = createConsumer();
        final List<KafkaMessage<T>> messages;
        var startOffsets = findOffsets(sourceTopic, startDate.getTime(), sourceKafkaConsumer);
        var endOffsets = findOffsets(sourceTopic, endDate.getTime(), sourceKafkaConsumer);
        boolean isAssigned = assignStartingOffsets(sourceKafkaConsumer, startOffsets);
        if (isAssigned) {
            messages = pollMessagesUntilEndOffsets(sourceKafkaConsumer, endOffsets, type);
            log.info("Found {} messages in topic {}", messages.size(), sourceTopic);
        } else {
            log.info("Could not be assigned to the topic {}", sourceTopic);
            messages = Collections.emptyList();
        }
        closeConsumer(sourceKafkaConsumer);
        return messages;
    }

    private boolean assignStartingOffsets(KafkaConsumer<String, Object> sourceKafkaConsumer,
                                          Map<TopicPartition, OffsetAndTimestamp> offsets) {
        if (!offsets.isEmpty()) {
            var nullOffsetsRemoved = offsets.entrySet().stream()
                    .filter(e -> !Objects.isNull(e.getValue()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            nullOffsetsRemoved.forEach((tp, offsetAndTimestamp) -> {
                sourceKafkaConsumer.assign(Collections.singletonList(tp));
                sourceKafkaConsumer.seek(tp, offsetAndTimestamp.offset());
            });
            return true;
        } else return false;
    }

    public <T> List<KafkaMessage<T>> pollMessagesUntilEndOffsets(KafkaConsumer<String, Object> consumer,
                                                           Map<TopicPartition, OffsetAndTimestamp> endOffsets,
                                                           Class<T> type) {
        var messages = new ArrayList<KafkaMessage<T>>();
        Map<String, Long> partitionsToEndOffsets = createEndOffsetMap(endOffsets);
        while (!partitionsToEndOffsets.isEmpty()) {
            final ConsumerRecords<String, Object> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            if (!consumerRecords.isEmpty()) {
                consumerRecords.forEach(record -> {
                    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                    String partitionId = topicPartition.toString();
                    if (partitionsToEndOffsets.containsKey(partitionId)) {
                        Long endOffset = partitionsToEndOffsets.get(partitionId);
                        if (endOffset.equals(INFINITE_OFFSET) || partitionsToEndOffsets.get(partitionId) >= record.offset()) {
                            T value = mapper.convertValue(record.value(), type);
                            messages.add(new KafkaMessage<T>(record.timestamp(), value));
                        } else {
                            partitionsToEndOffsets.remove(partitionId);
                        }
                    }
                });
            } else {
                break;
            }
            consumer.commitAsync();
        }
        return messages;
    }

    private Map<String, Long> createEndOffsetMap(Map<TopicPartition, OffsetAndTimestamp> endOffsets) {
        var offsetMap = new HashMap<String, Long>();
        endOffsets.forEach((key, value) -> {
            String partitionId = key.toString();
            if (!Objects.isNull(value)) {
                offsetMap.put(partitionId, value.offset());
            } else {
                offsetMap.put(partitionId, INFINITE_OFFSET);
            }
        });
        return offsetMap;
    }

    private void closeConsumer(KafkaConsumer<String, Object> consumer) {
        log.info("Closing consumer - topics: {} - metadata: {}",
                String.join(",", consumer.assignment().stream()
                        .map(TopicPartition::topic)
                        .collect(Collectors.toSet())),
                consumer.groupMetadata());
        consumer.close();
    }

    private KafkaConsumer<String, Object> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceKafkaHost);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "playbackService");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Object.class);
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        return new KafkaConsumer<>(props);
    }

    public Map<TopicPartition, OffsetAndTimestamp> findOffsets(String topicName, long timestampMs,
                                                               KafkaConsumer<String, Object> sourceKafkaConsumer) {
        List<PartitionInfo> partitionInfos = sourceKafkaConsumer.partitionsFor(topicName);
        List<TopicPartition> topicPartitionList = partitionInfos.stream()
                .map(info -> new TopicPartition(topicName, info.partition()))
                .collect(Collectors.toList());
        Map<TopicPartition, Long> partitionTimestampMap = topicPartitionList.stream()
                .collect(Collectors.toMap(tp -> tp, tp -> timestampMs));
        return sourceKafkaConsumer.offsetsForTimes(partitionTimestampMap);
    }
}
