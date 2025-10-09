package com.emqx.flink.connector;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EMQXSplitSerializer implements SimpleVersionedSerializer<EMQXSourceSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(EMQXSplitSerializer.class);
    private static final int VERSION = 0;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(EMQXSourceSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(split.clientid);
            out.writeUTF(split.topic);
            out.writeInt(split.qos);
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public EMQXSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        Preconditions.checkArgument(VERSION == version, "invalid serialized split version", version);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            String clientid = in.readUTF();
            String topic = in.readUTF();
            int qos = in.readInt();
            return new EMQXSourceSplit(clientid, topic, qos);
        }
    }
}
