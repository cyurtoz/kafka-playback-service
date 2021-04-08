package com.cyurtoz.handler;

import com.cyurtoz.model.KafkaMessage;

import java.util.Optional;

public interface CustomPlaybackHandler<T> {

    Optional<KafkaMessage<T>> handle(KafkaMessage<T> source);

    Class<T> getType();
}
