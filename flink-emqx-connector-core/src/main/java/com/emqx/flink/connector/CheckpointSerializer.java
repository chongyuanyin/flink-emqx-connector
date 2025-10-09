package com.emqx.flink.connector;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckpointSerializer implements SimpleVersionedSerializer<EMQXCheckpoint> {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointSerializer.class);
    private static final int VERSION = 0;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(EMQXCheckpoint checkpoint) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            int numRepeatableSubs = checkpoint.repeatableSubs.size();
            out.writeInt(numRepeatableSubs);
            for (Subscription sub : checkpoint.repeatableSubs) {
                out.writeInt(sub.getQoS());
                out.writeUTF(sub.toString());
            }

            int numPendingNonRepeatableSubs = checkpoint.pendingNonRepeatableSubs.size();
            out.writeInt(numPendingNonRepeatableSubs);
            for (Subscription sub : checkpoint.pendingNonRepeatableSubs) {
                out.writeInt(sub.getQoS());
                out.writeUTF(sub.toString());
            }

            int numAssignedReaders = checkpoint.assignedReaders.size();
            out.writeInt(numAssignedReaders);
            for (Map.Entry<Integer, List<Subscription>> entry : checkpoint.assignedReaders.entrySet()) {
                out.writeInt(entry.getKey());
                int numSubs = entry.getValue().size();
                out.writeInt(numSubs);
                for (Subscription sub : entry.getValue()) {
                    out.writeInt(sub.getQoS());
                    out.writeUTF(sub.toString());
                }
            }
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public EMQXCheckpoint deserialize(int version, byte[] serialized) throws IOException {
        Preconditions.checkArgument(VERSION == version, "invalid serialized checkpoint version", version);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            int i;
            int qos;
            String topic;
            Subscription sub;

            int numRepeatableSubs = in.readInt();
            List<Subscription> repeatableSubs = new ArrayList<>();
            for (i = 1; i <= numRepeatableSubs; i++) {
                qos = in.readInt();
                topic = in.readUTF();
                sub = SubscriptionUtils.parse(topic, qos);
                repeatableSubs.add(sub);
            }

            int numPendingNonRepeatableSubs = in.readInt();
            List<Subscription> pendingNonRepeatableSubs = new ArrayList<>();
            for (i = 1; i <= numPendingNonRepeatableSubs; i++) {
                qos = in.readInt();
                topic = in.readUTF();
                sub = SubscriptionUtils.parse(topic, qos);
                pendingNonRepeatableSubs.add(sub);
            }

            int subTaskId;
            int numSubs;
            int numAssignedReaders = in.readInt();
            int j;
            Map<Integer, List<Subscription>> assignedReaders = new TreeMap<>();
            for (i = 1; i <= numAssignedReaders; i++) {
                subTaskId = in.readInt();
                numSubs = in.readInt();
                List<Subscription> subs = new ArrayList<>();
                for (j = 1; j <= numSubs; j++) {
                    qos = in.readInt();
                    topic = in.readUTF();
                    sub = SubscriptionUtils.parse(topic, qos);
                    subs.add(sub);
                }
                assignedReaders.put(subTaskId, subs);
            }

            return new EMQXCheckpoint(repeatableSubs, pendingNonRepeatableSubs, assignedReaders);
        }
    }
}
