package com.cyurtoz.service;

import com.cyurtoz.handler.CustomPlaybackHandler;
import com.cyurtoz.kafka.SourceKafkaService;
import com.cyurtoz.kafka.TargetKafkaService;
import com.cyurtoz.model.KafkaMessage;
import com.cyurtoz.model.PlaybackInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
@Slf4j
public class PlaybackService {

    private final ConcurrentHashMap<PlaybackInfo, ScheduledExecutorService> ongoingPlaybacks;
    private final TargetKafkaService targetKafkaService;
    private final SourceKafkaService sourceKafkaService;

    public PlaybackService(TargetKafkaService targetKafkaService, SourceKafkaService sourceKafkaService) {
        this.ongoingPlaybacks = new ConcurrentHashMap<>();
        this.targetKafkaService = targetKafkaService;
        this.sourceKafkaService = sourceKafkaService;
    }

    public String createNewPlayback(String sourceTopic, String destinationTopic, double playbackSpeed, Date startDate, Date endDate) {
        var messages = initMessages(sourceTopic, destinationTopic, startDate, endDate, Object.class);
        var info = PlaybackInfo.create(sourceTopic, destinationTopic, playbackSpeed);
        schedulePlaybackMessages(messages, info);
        return info.getId();
    }

    public <T> String createNewPlaybackWithHandler(String sourceTopic, String destinationTopic, double playbackSpeed,
                                                   Date startDate, Date endDate, CustomPlaybackHandler<T> handler) {

        List<KafkaMessage<T>> kafkaMessages = initMessages(sourceTopic, destinationTopic, startDate, endDate, handler.getType());
        var mapped = kafkaMessages.stream().map(handler::handle).collect(Collectors.toList());
        var info = PlaybackInfo.create(sourceTopic, destinationTopic, playbackSpeed);
        var filtered = mapped.stream().filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
        schedulePlaybackMessages(filtered, info);
        return info.getId();
    }

    private <T> List<KafkaMessage<T>> initMessages(String sourceTopic, String destinationTopic, Date startDate, Date endDate, Class<T> type) {
        validateDestination(destinationTopic);
        List<KafkaMessage<T>> messages = sourceKafkaService.findMessagesBetween(sourceTopic, startDate, endDate, type);
        validateMessages(messages);
        return messages;
    }

    private <T> void schedulePlaybackMessages(List<KafkaMessage<T>> messages, PlaybackInfo playbackInfo) {
        int size = messages.size();
        var executor = Executors.newScheduledThreadPool(1);
        ongoingPlaybacks.put(playbackInfo, executor);
        for (int index = 0; index < size; index++) {
            KafkaMessage<T> first = messages.get(0);
            KafkaMessage<T> current = messages.get(index);
            long diff = current.getTimestamp() - first.getTimestamp();
            long delay = (long) ((double) diff / playbackInfo.getSpeed());
            scheduleKafkaMessage(executor, playbackInfo, delay, current.getPayload());
            if (index == size - 1) {
                scheduleFinishMessage(executor, playbackInfo, delay);
            }
        }
        executor.shutdown();
    }

    private void scheduleFinishMessage(ScheduledExecutorService executor, PlaybackInfo playbackInfo, long delay) {
        executor.schedule(() -> ongoingPlaybacks.remove(playbackInfo), delay + 100L, TimeUnit.MILLISECONDS);
    }

    private void scheduleKafkaMessage(ScheduledExecutorService executor, PlaybackInfo playbackInfo, long delay, Object message) {
        executor.schedule(() -> targetKafkaService.send(playbackInfo.getTargetTopic(), message), delay, TimeUnit.MILLISECONDS);
    }

    private boolean isDestinationNotUsed(String destination) {
        return ongoingPlaybacks.keySet().stream().noneMatch(e -> e.getTargetTopic().equals(destination));
    }

    public List<PlaybackInfo> listPlaybacks() {
        return new ArrayList<>(ongoingPlaybacks.keySet());
    }

    public void stop(String id) {
        Optional<PlaybackInfo> first = ongoingPlaybacks.keySet().stream().filter(e -> e.getId().equals(id)).findFirst();
        first.ifPresent(info -> {
            List<Runnable> runnables = ongoingPlaybacks.get(info).shutdownNow();
            ongoingPlaybacks.remove(info);
            targetKafkaService.stopForTopic(info.getTargetTopic());
            log.info("Stopped {} - {} messages were cancelled", id, runnables.size());
        });
    }

    private void validateDestination(String destinationTopic) {
        if (!isDestinationNotUsed(destinationTopic))
            throw new RuntimeException("Destination topic is used.");
    }

    private <T> void validateMessages(List<KafkaMessage<T>> messages) {
        if (messages.isEmpty()) throw new RuntimeException("No data could be found in source topic.");
    }
}
