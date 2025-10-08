package com.emqx.flink.connector;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class EMQXCheckpoint implements Serializable {
    protected List<Subscription> repeatableSubs;
    protected List<Subscription> pendingNonRepeatableSubs;
    protected Map<Integer, List<Subscription>> assignedReaders;

    EMQXCheckpoint(
            List<Subscription> repeatableSubs,
            List<Subscription> pendingNonRepeatableSubs,
            Map<Integer, List<Subscription>> assignedReaders) {
        this.repeatableSubs = repeatableSubs;
        this.pendingNonRepeatableSubs = pendingNonRepeatableSubs;
        this.assignedReaders = assignedReaders;
    }
}
