package com.emqx.flink.connector;

public interface Subscription {
    boolean isRepeatable();

    int getQoS();
}
