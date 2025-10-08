package com.emqx.flink.connector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

public class EMQXSplitEnumerator implements SplitEnumerator<EMQXSourceSplit, EMQXCheckpoint> {
    private static final Logger LOG = LoggerFactory.getLogger(EMQXSplitEnumerator.class);

    private SplitEnumeratorContext<EMQXSourceSplit> context;
    private String baseClientid;
    private List<Subscription> repeatableSubs = new ArrayList<>();
    private List<Subscription> pendingNonRepeatableSubs = new ArrayList<>();
    private Map<Integer, List<Subscription>> assignedReaders = new TreeMap<>();

    EMQXSplitEnumerator(
            SplitEnumeratorContext<EMQXSourceSplit> context,
            String baseClientid,
            List<Subscription> subscriptions) {
        this.context = context;
        this.baseClientid = baseClientid;
        for (Subscription sub : subscriptions) {
            if (sub.isRepeatable()) {
                repeatableSubs.add(sub);
            } else {
                pendingNonRepeatableSubs.add(sub);
            }
        }
        LOG.debug("repeatable subs: {}; pending non repeatable subs: {}", repeatableSubs, pendingNonRepeatableSubs);
    }

    protected EMQXSplitEnumerator(
            SplitEnumeratorContext<EMQXSourceSplit> context,
            String baseClientid,
            List<Subscription> repeatableSubs,
            List<Subscription> pendingNonRepeatableSubs,
            Map<Integer, List<Subscription>> assignedReaders) {
        this.context = context;
        this.baseClientid = baseClientid;
        this.repeatableSubs = repeatableSubs;
        this.pendingNonRepeatableSubs = pendingNonRepeatableSubs;
        this.assignedReaders = assignedReaders;
    }

    @Override
    public void start() {
        // TODO Auto-generated method stub
        LOG.debug("start");
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void addReader(int subTaskId) {
        LOG.debug("addReader: {}", subTaskId);
    }

    @Override
    public void addSplitsBack(List<EMQXSourceSplit> splits, int subTaskId) {
        LOG.debug("adding splits back from {}: {}", subTaskId, splits);
        // TODO
    }

    @Override
    public void handleSplitRequest(int subTaskId, @Nullable String requesterHostname) {
        int currentParallelism = context.currentParallelism();
        LOG.debug("handleSplitRequest: {}@{}; parallelism: {}", subTaskId, requesterHostname,
                currentParallelism);
        List<Subscription> subs = new ArrayList<>();
        // Dish out a fraction of the pending non-repeatable subs.
        int toTake = Integer.min(
                pendingNonRepeatableSubs.size(),
                Integer.max(1, (int) Math.ceil(pendingNonRepeatableSubs.size() / currentParallelism)));
        List<Subscription> uniqueSubs = pendingNonRepeatableSubs.subList(0, toTake);
        uniqueSubs.forEach((sub) -> subs.add(sub));
        uniqueSubs.clear();
        // Also give one copy of each repeatable one.
        repeatableSubs.forEach((sub) -> subs.add(sub));
        assignedReaders.put(subTaskId, subs);
        // SplitsAssignment<EMQXSourceSplit> assignments = new SplitsAssign
        List<EMQXSourceSplit> splits = subs.stream()
                .map((sub) -> new EMQXSourceSplit(mkClientid(subTaskId), sub.toString(), sub.getQoS()))
                .collect(Collectors.toList());
        context.assignSplits(new SplitsAssignment<>(Collections.singletonMap(subTaskId, splits)));
        LOG.info("assigned splits to reader {}: {}", subTaskId, subs);
    }

    @Override
    public EMQXCheckpoint snapshotState(long checkpointId) throws Exception {
        LOG.debug("snapshot: {}", checkpointId);
        return new EMQXCheckpoint(repeatableSubs, pendingNonRepeatableSubs, assignedReaders);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.debug("checkpoint complete: {}", checkpointId);
        SplitEnumerator.super.notifyCheckpointComplete(checkpointId);
    }

    String mkClientid(int subTaskId) {
        return EMQXSource.mkClientid(baseClientid, subTaskId);
    }
}
