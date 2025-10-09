package com.emqx.flink.connector;

import org.apache.flink.shaded.curator5.com.google.common.base.Preconditions;

public class SubscriptionUtils {
    static Subscription parse(String topicFilter, int qos) {
        Preconditions.checkNotNull(topicFilter);
        Preconditions.checkArgument(!topicFilter.isEmpty(), "topic filter must not be empty");
        Preconditions.checkArgument(0 <= qos && qos <= 2, "invalid qos");
        if (topicFilter.startsWith("$q/")) {
            return new QueueSubscription(topicFilter, qos);
        } else if (topicFilter.startsWith("$share/")) {
            String[] parts = topicFilter.split("/", 3);
            String groupName = parts[1];
            String filter = parts[2];
            return new SharedSubscription(groupName, filter, qos);
        } else {
            return new SimpleSubscription(topicFilter, qos);
        }
    }
}
