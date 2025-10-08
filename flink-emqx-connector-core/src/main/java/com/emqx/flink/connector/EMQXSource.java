package com.emqx.flink.connector;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EMQXSource<OUT>
        implements Source<EMQXMessage<OUT>, EMQXSourceSplit, EMQXCheckpoint>, ResultTypeQueryable<EMQXMessage<OUT>> {
    private static final Logger LOG = LoggerFactory.getLogger(EMQXSource.class);

    protected String brokerHost;
    protected int brokerPort;
    protected String baseClientid;
    protected String username;
    protected String password;
    protected String groupName;
    protected String topicFilter;
    protected int qos;
    protected DeserializationSchema<OUT> deserializer;

    public EMQXSource(String brokerHost, int brokerPort, String baseClientid, String groupName, String topicFilter, int qos,
            DeserializationSchema<OUT> deserializer) {
        this(brokerHost, brokerPort, baseClientid, null, null, groupName, topicFilter, qos, deserializer);
    }

    public EMQXSource(String brokerHost, int brokerPort, String baseClientid, String username, String password,
            String groupName, String topicFilter, int qos, DeserializationSchema<OUT> deserializer) {
        Preconditions.checkArgument(0 <= qos && qos <= 2, "invalid qos: %", qos);
        // TODO: validate group name and clientid
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.baseClientid = baseClientid;
        this.username = username;
        this.password = password;
        this.groupName = groupName;
        this.topicFilter = topicFilter;
        this.qos = qos;
        this.deserializer = deserializer;
    }

    @Override
    public SplitEnumerator<EMQXSourceSplit, EMQXCheckpoint> createEnumerator(
            SplitEnumeratorContext<EMQXSourceSplit> context) throws Exception {
        return new EMQXSplitEnumerator(context, baseClientid);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SimpleVersionedSerializer<EMQXSourceSplit> getSplitSerializer() {
        return new EMQXSplitSerializer();
    }

    @Override
    public SourceReader<EMQXMessage<OUT>, EMQXSourceSplit> createReader(SourceReaderContext context) throws Exception {
        int subTaskId = context.getIndexOfSubtask();
        String newClientid = mkClientid(baseClientid, subTaskId);
        LOG.debug("Starting Source Reader; clientid: {}; group name: {}", newClientid, groupName);
        return new EMQXSourceReader<>(context, brokerHost, brokerPort, newClientid, username, password, groupName, topicFilter, qos, deserializer);
    }

    @Override
    public SimpleVersionedSerializer<EMQXCheckpoint> getEnumeratorCheckpointSerializer() {
        LOG.debug("getEnumeratorCheckpointSerializer");
        return new SimpleSerializer<EMQXCheckpoint>();
    }

    @Override
    public TypeInformation<EMQXMessage<OUT>> getProducedType() {
        // return deserializer.getProducedType();
        return TypeInformation.of(new TypeHint<EMQXMessage<OUT>>() {
        });
    }

    @Override
    public SplitEnumerator<EMQXSourceSplit, EMQXCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<EMQXSourceSplit> enumContext, EMQXCheckpoint checkpoint) throws Exception {
        LOG.debug("restoreEnumerator");
        return null;
    }

    static public String mkClientid(String baseClientid, int subTaskId) {
        return String.format("%s%d", baseClientid, subTaskId);
    }
}
