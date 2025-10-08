package com.emqx.flink.connector;

import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

public class EMQXMessage<PAYLOAD> {
    public String topic;
    public int qos;
    public boolean retained;
    public MqttProperties properties;
    public PAYLOAD payload;

    public EMQXMessage(String topic, int qos, boolean retained, MqttProperties properties, PAYLOAD payload) {
        this.topic = topic;
        this.qos = qos;
        this.retained = retained;
        this.properties = properties;
        this.payload = payload;
    }

    @Override
    public String toString() {
        return String.format("EMQXMessage{topic: %s, qos: %d, retained: %s, properties: %s, payload: %s}",
                topic, qos, retained, properties, payload);
    }
}
