package com.emqx.flink.connector;

import java.io.Serializable;

public class SharedSubscription implements Subscription, Serializable {
    public String groupName;
    public String topic;
    public int qos;

    public SharedSubscription(String groupName, String topic, int qos) {
        // TODO: validate group name
        SimpleSubscription.validateTopic(topic);
        SimpleSubscription.validateQoS(qos);
        this.groupName = groupName;
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
        return String.format("$share/%s/%s", groupName, topic);
    }
}
