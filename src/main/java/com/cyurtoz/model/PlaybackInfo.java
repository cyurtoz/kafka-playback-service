package com.cyurtoz.model;

import lombok.Data;

import java.util.Objects;
import java.util.UUID;

@Data
public class PlaybackInfo {

    private String id;
    private String hostTopic;
    private String targetTopic;
    private double speed;

    public static PlaybackInfo create(String hostTopic, String destinationTopic, double playbackSpeed) {
        PlaybackInfo playbackInfo = new PlaybackInfo();
        playbackInfo.id = UUID.randomUUID().toString();
        playbackInfo.hostTopic = hostTopic;
        playbackInfo.targetTopic = destinationTopic;
        playbackInfo.speed = playbackSpeed;
        return playbackInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlaybackInfo that = (PlaybackInfo) o;
        return speed == that.speed &&
                Objects.equals(id, that.id) &&
                Objects.equals(hostTopic, that.hostTopic) &&
                Objects.equals(targetTopic, that.targetTopic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
