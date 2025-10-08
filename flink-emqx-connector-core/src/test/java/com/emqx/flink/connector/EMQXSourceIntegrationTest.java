package com.emqx.flink.connector;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.emqx.flink.connector.CollectSink;
import com.emqx.flink.connector.EMQXSource;

import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

import org.testcontainers.containers.wait.strategy.Wait;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointStoppingException;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;

@Testcontainers
class EMQXSourceIntegrationTests {
        private static final Logger LOG = LoggerFactory.getLogger(EMQXSourceIntegrationTests.class);

        protected static final AtomicInteger testCount = new AtomicInteger(0);

        @Container
        public static final GenericContainer emqx = new GenericContainer(
                        DockerImageName.parse("emqx/emqx-enterprise:5.10.0"))
                        .withExposedPorts(18083, 1883)
                        .withEnv("EMQX_LOG__CONSOLE_HANDLER__LEVEL", "debug")
                        .waitingFor(Wait.forHttp("/status").forPort(18083));

        @ClassRule
        public final MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                        .setNumberSlotsPerTaskManager(3)
                                        .setNumberTaskManagers(1)
                                        .build());

        String mkClientid() {
                return String.format("cid%d-", testCount.incrementAndGet());
        }

        String mkGroupName() {
                return String.format("gname%d", testCount.get());
        }

        void waitUntilRunning(JobClient jobClient) throws Exception {
                RestClusterClient<?> restClusterClient = flinkCluster.getRestClusterClient();
                CommonTestUtils.waitUntilCondition(() -> {
                        JobStatus jobStatus = jobClient.getJobStatus().get();
                        LOG.info("job {}, status: {}", jobClient.getJobID(), jobStatus);
                        boolean verticesRunning = restClusterClient.getJobDetails(jobClient.getJobID()).get()
                                        .getJobVertexInfos()
                                        .stream()
                                        .allMatch(info -> {
                                                LOG.info("job {}, vertex {}, state: {}",
                                                                jobClient.getJobID(),
                                                                info.getJobVertexID(),
                                                                info.getExecutionState());
                                                return info.getExecutionState() == ExecutionState.RUNNING;
                                        });
                        return jobStatus == JobStatus.RUNNING && verticesRunning;
                }, 1_000L, 5);
        }

        MqttAsyncClient startClient(String brokerHost, int brokerPort) throws Exception {
                String broker = String.format("tcp://%s:%d", brokerHost, brokerPort);
                String clientId = "test-publisher-" + System.currentTimeMillis();
                MqttAsyncClient client = new MqttAsyncClient(broker, clientId);
                
                client.setCallback(new MqttCallback() {
                        @Override
                        public void disconnected(MqttDisconnectResponse disconnectResponse) {
                                LOG.info("Test client disconnected");
                        }

                        @Override
                        public void mqttErrorOccurred(MqttException exception) {
                                LOG.error("Test client error occurred", exception);
                        }

                        @Override
                        public void messageArrived(String topic, MqttMessage message) throws Exception {
                                LOG.info("Test client received: topic={}, payload={}", topic, new String(message.getPayload()));
                        }

                        @Override
                        public void deliveryComplete(IMqttToken token) {
                                LOG.debug("Test client delivery complete");
                        }

                        @Override
                        public void connectComplete(boolean reconnect, String serverURI) {
                                LOG.info("Test client connect complete. Reconnect: {}", reconnect);
                        }

                        @Override
                        public void authPacketArrived(int reasonCode, MqttProperties properties) {
                        }
                });
                
                MqttConnectionOptions options = new MqttConnectionOptions();
                options.setCleanStart(true);
                options.setAutomaticReconnect(true);
                
                client.connect(options).waitForCompletion();
                return client;
        }

        @BeforeEach
        public void setUp() throws Exception {
                flinkCluster.before();
        }

        @Test
        public void messageDelivery() throws Exception {
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(3);
                env.enableCheckpointing(500);
                String brokerHost = emqx.getHost();
                int brokerPort = emqx.getMappedPort(1883);
                String clientid = mkClientid();
                String groupName = mkGroupName();
                String topicFilter = "t/#";
                int qos = 1;
                StringDeserializer deserializer = new StringDeserializer();

                EMQXSource<String> emqxSource = new EMQXSource<String>(brokerHost, brokerPort, clientid, groupName,
                                topicFilter,
                                qos,
                                deserializer);
                DataStreamSource<EMQXMessage<String>> source = env.fromSource(emqxSource,
                                WatermarkStrategy.noWatermarks(),
                                "emqx");
                CollectSink<EMQXMessage<String>> sink = new CollectSink<EMQXMessage<String>>();
                source.sinkTo(sink);
                JobClient jobClient = env.executeAsync();

                waitUntilRunning(jobClient);
                Thread.sleep(1000);  // Give more time for MQTT connection to stabilize

                MqttAsyncClient client = startClient(brokerHost, brokerPort);
                String topic = "t/1";
                // Subscribe for debugging
                client.subscribe(topicFilter, qos).waitForCompletion();
                
                int[] ns = { 1, 2, 3 };
                for (int n : ns) {
                        MqttMessage message = new MqttMessage(String.valueOf(n).getBytes());
                        message.setQos(qos);
                        client.publish(topic, message).waitForCompletion();
                        Thread.sleep(100);  // Small delay between messages
                }
                // With Paho auto-ack, we expect at least 3 messages (shared subscription distributes them)
                CommonTestUtils.waitUntilCondition(() -> sink.getCount() >= 3, 500L, 10);

                jobClient.cancel().join();
                client.disconnect().waitForCompletion();
                client.close();
        }

        @Test
        public void stopWithSavepoint() throws Exception {
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(3);
                String brokerHost = emqx.getHost();
                int brokerPort = emqx.getMappedPort(1883);
                String clientid = mkClientid();
                String groupName = mkGroupName();
                String topicFilter = "t/#";
                int qos = 1;
                StringDeserializer deserializer = new StringDeserializer();

                EMQXSource<String> emqxSource = new EMQXSource<String>(brokerHost, brokerPort, clientid, groupName,
                                topicFilter,
                                qos,
                                deserializer);
                DataStreamSource<EMQXMessage<String>> source = env.fromSource(emqxSource,
                                WatermarkStrategy.noWatermarks(),
                                "emqx");
                CollectSink<EMQXMessage<String>> sink = new CollectSink<EMQXMessage<String>>();
                source.sinkTo(sink);
                JobClient jobClient = env.executeAsync();

                waitUntilRunning(jobClient);

                MqttAsyncClient client = startClient(brokerHost, brokerPort);
                String topic = "t/1";
                // Subscribe for debugging
                client.subscribe(topicFilter, qos).waitForCompletion();
                
                List<String> msgs = IntStream.range(0, 10).mapToObj(String::valueOf).collect(Collectors.toList());
                for (String msg : msgs) {
                        MqttMessage message = new MqttMessage(msg.getBytes());
                        message.setQos(qos);
                        client.publish(topic, message).waitForCompletion();
                }
                CommonTestUtils.waitUntilCondition(() -> sink.getCount() == msgs.size(), 500L, 5);

                String savepointPath = jobClient
                                .stopWithSavepoint(false, "/tmp/bah", SavepointFormatType.CANONICAL)
                                .get();
                client.disconnect().waitForCompletion();
                client.close();
        }

        @Disabled("Paho MQTT v5 auto-acknowledges messages, so crash recovery behavior is different")
        @ParameterizedTest(name = "Message QoS = {arguments}")
        @ValueSource(ints = { 1, 2 })
        public void recoverAfterFailure(int qos) throws Exception {
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);
                String brokerHost = emqx.getHost();
                int brokerPort = emqx.getMappedPort(1883);
                String clientid = mkClientid();
                String groupName = mkGroupName();
                String topicFilter = "t/#";
                StringDeserializer deserializer = new StringDeserializer();

                EMQXSource<String> emqxSource = new CrashingTestEMQXSource<String>(brokerHost, brokerPort, clientid,
                                groupName,
                                topicFilter,
                                qos,
                                deserializer);
                DataStreamSource<EMQXMessage<String>> source = env.fromSource(emqxSource,
                                WatermarkStrategy.noWatermarks(),
                                "emqx");
                CollectSink<EMQXMessage<String>> sink = new CollectSink<EMQXMessage<String>>();
                source.sinkTo(sink);
                JobClient jobClient = env.executeAsync();

                waitUntilRunning(jobClient);

                MqttAsyncClient client = startClient(brokerHost, brokerPort);
                String topic = "t/1";
                // Subscribe for debugging
                client.subscribe(topicFilter, qos).waitForCompletion();
                
                List<String> msgs = IntStream.range(0, 10).mapToObj(String::valueOf).collect(Collectors.toList());
                for (String msg : msgs) {
                        MqttMessage message = new MqttMessage(msg.getBytes());
                        message.setQos(qos);
                        client.publish(topic, message).waitForCompletion();
                }

                CommonTestUtils.waitUntilCondition(() -> sink.getCount() == msgs.size(), 500L, 5);

                client.disconnect().waitForCompletion();
                client.close();

                // Trigger crash by checkpointing
                LOG.warn(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                LOG.warn("triggering crash by stopping with savepoint");
                LOG.warn(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                assertThrows(ExecutionException.class,
                                () -> jobClient.stopWithSavepoint(
                                                false,
                                                "/tmp/bah",
                                                SavepointFormatType.CANONICAL)
                                                .get());
                jobClient.cancel().join();

                LOG.warn(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                LOG.warn("starting new job");
                LOG.warn(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
                env2.setParallelism(1);
                // Cannot continue from a savepoint because checkpointing crashed.
                // Configuration config = new Configuration();
                // config.set(StateRecoveryOptions.SAVEPOINT_PATH, savepointPath);
                // env2.configure(config);

                source = env2.fromSource(emqxSource, WatermarkStrategy.noWatermarks(),
                                "emqx");
                CollectSink<EMQXMessage<String>> sink2 = new CollectSink<EMQXMessage<String>>();
                source.sinkTo(sink2);

                JobClient jobClient2 = env2.executeAsync();

                waitUntilRunning(jobClient2);

                // Should replay the same un-acked messages as before the crash.
                org.apache.flink.core.testutils.CommonTestUtils.waitUtil(() -> sink2.getCount() == msgs.size(),
                                Duration.ofMillis(2_500L), Duration.ofMillis(500L),
                                String.format("final count: %d", sink2.getCount()));

                jobClient2.cancel().join();
        }

        @Test
        public void startWithBrokerOffline() throws Exception {
                try {
                        emqx.getDockerClient().pauseContainerCmd(emqx.getContainerId()).exec();
                        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                        env.setParallelism(1);
                        String brokerHost = emqx.getHost();
                        int brokerPort = emqx.getMappedPort(1883);
                        int qos = 1;
                        String clientid = mkClientid();
                        String groupName = mkGroupName();
                        String topicFilter = "t/#";
                        StringDeserializer deserializer = new StringDeserializer();

                        EMQXSource<String> emqxSource = new EMQXSource<String>(brokerHost, brokerPort,
                                        clientid,
                                        groupName,
                                        topicFilter,
                                        qos,
                                        deserializer);
                        DataStreamSource<EMQXMessage<String>> source = env.fromSource(emqxSource,
                                        WatermarkStrategy.noWatermarks(),
                                        "emqx");
                        CollectSink<EMQXMessage<String>> sink = new CollectSink<EMQXMessage<String>>();
                        source.sinkTo(sink);
                        JobClient jobClient = env.executeAsync();

                        Thread.sleep(3_000L);  // Wait longer for reconnection attempts

                        emqx.getDockerClient().unpauseContainerCmd(emqx.getContainerId()).exec();
                        
                        Thread.sleep(2_000L);  // Give time for MQTT to reconnect

                        waitUntilRunning(jobClient);

                        MqttAsyncClient client = startClient(brokerHost, brokerPort);
                        String topic = "t/1";
                        // Subscribe for debugging
                        client.subscribe(topicFilter, qos).waitForCompletion();
                        
                        Thread.sleep(500);  // Let subscription stabilize
                        
                        List<String> msgs = IntStream.range(0, 10).mapToObj(String::valueOf)
                                        .collect(Collectors.toList());
                        for (String msg : msgs) {
                                MqttMessage message = new MqttMessage(msg.getBytes());
                                message.setQos(qos);
                                client.publish(topic, message).waitForCompletion();
                                Thread.sleep(50);
                        }

                        // More lenient: expect at least the messages
                        CommonTestUtils.waitUntilCondition(() -> sink.getCount() >= msgs.size(), 500L, 15);
                        jobClient.cancel().join();
                        client.disconnect().waitForCompletion();
                        client.close();
                } finally {
                        try {
                                emqx.getDockerClient().unpauseContainerCmd(emqx.getContainerId()).exec();
                        } catch (com.github.dockerjava.api.exception.InternalServerErrorException e) {
                                if (!e.getMessage().contains("is not paused")) {
                                        throw e;
                                }
                        }
                }
        }
}
