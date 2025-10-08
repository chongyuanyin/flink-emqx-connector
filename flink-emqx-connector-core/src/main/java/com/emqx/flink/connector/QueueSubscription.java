package com.emqx.flink.connector;

import java.io.Serializable;

public class QueueSubscription implements Subscription, Serializable {
    public String topic;
    public int qos;

    public QueueSubscription(String topic, int qos) {
        SimpleSubscription.validateTopic(topic);
        SimpleSubscription.validateQoS(qos);
        this.topic = topic;
        this.qos = qos;
    }

    @Override
    public boolean isRepeatable() {
        return true;
    }

    @Override
    public int getQoS() {
        return qos;
    }

    @Override
    public String toString() {
        return String.format("$q/%s", topic);
    }
}
