package com.emqx.flink.connector;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.util.List;

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
    protected DeserializationSchema<OUT> deserializer;
    protected List<Subscription> subscriptions;

    public EMQXSource(String brokerHost, int brokerPort, String baseClientid, List<Subscription> subscriptions,
            DeserializationSchema<OUT> deserializer) {
        this(brokerHost, brokerPort, baseClientid, null, null, subscriptions, deserializer);
    }

    public EMQXSource(String brokerHost, int brokerPort, String baseClientid, String username, String password,
            List<Subscription> subscriptions, DeserializationSchema<OUT> deserializer) {
        // TODO: validate clientid
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.baseClientid = baseClientid;
        this.username = username;
        this.password = password;
        this.deserializer = deserializer;
        this.subscriptions = subscriptions;
    }

    @Override
    public SplitEnumerator<EMQXSourceSplit, EMQXCheckpoint> createEnumerator(
            SplitEnumeratorContext<EMQXSourceSplit> context) throws Exception {
        LOG.info("creating enumerator with base clientid {} and subscriptions {}", baseClientid, subscriptions);
        return new EMQXSplitEnumerator(context, baseClientid, subscriptions);
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
        LOG.debug("Creating Source Reader; clientid: {}", newClientid);
        return new EMQXSourceReader<>(context, brokerHost, brokerPort, newClientid, username, password, deserializer);
    }

    @Override
    public SimpleVersionedSerializer<EMQXCheckpoint> getEnumeratorCheckpointSerializer() {
        LOG.debug("getEnumeratorCheckpointSerializer");
        return new CheckpointSerializer();
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
        LOG.debug("restoreEnumerator: {} {}\n  {}", enumContext, checkpoint, enumContext.registeredReaders());
        return new EMQXSplitEnumerator(enumContext, baseClientid, checkpoint.repeatableSubs,
                checkpoint.pendingNonRepeatableSubs, checkpoint.assignedReaders);
    }

    static public String mkClientid(String baseClientid, int subTaskId) {
        return String.format("%s%d", baseClientid, subTaskId);
    }
}
