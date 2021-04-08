package com.cyurtoz.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class KafkaMessage<T> {
    private long timestamp;
    private T payload;

}
