package com.emqx.flink.connector;

import java.io.Serializable;

import org.apache.flink.api.connector.source.SourceSplit;

class EMQXSourceSplit implements SourceSplit, Serializable {
    protected String clientid;
    protected String topic;
    protected int qos;

    EMQXSourceSplit(String clientid, String topic, int qos) {
        this.clientid = clientid;
        this.topic = topic;
        this.qos = qos;
    }

    public String splitId() {
        return this.clientid + ":" + String.valueOf(qos) + ":" + this.topic;
    }

    @Override
    public String toString() {
        return String.format("EMQXSourceSplit(%s, %s, %d)", clientid, topic, qos);
    }
}
