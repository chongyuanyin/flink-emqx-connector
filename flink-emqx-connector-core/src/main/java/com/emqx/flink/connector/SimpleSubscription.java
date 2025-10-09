package com.emqx.flink.connector;

import java.io.Serializable;

import org.apache.flink.shaded.curator5.org.apache.curator.shaded.com.google.common.base.Preconditions;

public class SimpleSubscription implements Subscription, Serializable {
    public String topic;
    public int qos;

    public SimpleSubscription(String topic, int qos) {
        validateTopic(topic);
        validateQoS(qos);
        this.topic = topic;
        this.qos = qos;
    }

    static public void validateTopic(String topic) {
        Preconditions.checkNotNull(topic, "topic must not be null");
        Preconditions.checkArgument(!topic.isEmpty(), "topic must not be empty");
    }

    static public void validateQoS(int qos) {
        Preconditions.checkArgument(0 <= qos && qos <= 2, "invalid qos");
    }

    @Override
    public boolean isRepeatable() {
        return false;
    }

    @Override
    public int getQoS() {
        return qos;
    }

    @Override
    public String toString() {
        return topic;
    }
}
