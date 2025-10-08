package com.emqx.flink.connector;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrashingTestEMQXSource<OUT> extends EMQXSource<OUT> {
    private static final Logger LOG = LoggerFactory.getLogger(CrashingTestEMQXSource.class);

    CrashingTestEMQXSource(String brokerHost, int brokerPort, String baseClientid, String groupName, String topicFilter,
            int qos,
            DeserializationSchema<OUT> deserializer) {
        super(brokerHost, brokerPort, baseClientid, groupName,
                topicFilter,
                qos,
                deserializer);
    }

    @Override
    public SourceReader<EMQXMessage<OUT>, EMQXSourceSplit> createReader(SourceReaderContext context) throws Exception {
        int subTaskId = context.getIndexOfSubtask();
        String newClientid = mkClientid(baseClientid, subTaskId);
        LOG.debug("Starting Crashing Source Reader; clientid: {}; group name: {}", newClientid, groupName);
        return new EMQXSourceReader<>(context, brokerHost, brokerPort, newClientid, username, password, groupName, topicFilter, qos,
                deserializer) {
            @Override
            public List<EMQXSourceSplit> snapshotState(long checkpointId) {
                LOG.warn("going to crash now");
                throw new RuntimeException("Mocked error");
            }
        };
    }
}
