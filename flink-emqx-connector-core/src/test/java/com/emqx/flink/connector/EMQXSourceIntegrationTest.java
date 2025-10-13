package com.emqx.flink.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpClient.Version;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
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
import org.testcontainers.images.builder.Transferable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.function.SupplierWithException;
import org.assertj.core.util.Arrays;
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
                        DockerImageName.parse("emqx/emqx-enterprise:6.0.0"))
                        .withExposedPorts(18083, 1883)
                        .withEnv("EMQX_LOG__CONSOLE_HANDLER__LEVEL", "debug")
                        .withEnv("EMQX_MQ__ENABLE", "true")
                        .withCopyToContainer(Transferable.of("test:secret"), "/etc/emqx/bootstrap.txt")
                        .withEnv("EMQX_API_KEY__BOOTSTRAP_FILE", "/etc/emqx/bootstrap.txt")
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

        void warnBanner(String message) {
                String border = "\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n";
                LOG.warn(border + message + border);
        }

        void setupEMQXQueue() throws Exception {
                HttpClient client = HttpClient.newBuilder().version(Version.HTTP_1_1).build();
                String auth = Base64.getEncoder().encodeToString("test:secret".getBytes());
                URI uri = new URI(String.format("http://%s:%d/api/v5/message_queues/queues",
                                emqx.getHost(),
                                emqx.getMappedPort(18083)));
                HttpRequest req = HttpRequest
                                .newBuilder(uri)
                                .header("Authorization", "Basic " + auth)
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(
                                                "{\"topic_filter\": \"queue/#\", \"is_lastvalue\": false}"))
                                .build();
                HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
                LOG.info("create queue response: {} {}", resp.statusCode(), resp.body());
                assertEquals(200, resp.statusCode());
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
                                LOG.info("Test client received: topic={}, payload={}", topic,
                                                new String(message.getPayload()));
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
                List<Subscription> subscriptions = new ArrayList<>();
                subscriptions.add(new SharedSubscription(groupName, topicFilter, qos));
                StringDeserializer deserializer = new StringDeserializer();

                EMQXSource<String> emqxSource = new EMQXSource<String>(brokerHost, brokerPort, clientid,
                                subscriptions, 
                                SslOption.SSL_DISABLED, null, null, null,
                                deserializer);
                DataStreamSource<EMQXMessage<String>> source = env.fromSource(emqxSource,
                                WatermarkStrategy.noWatermarks(),
                                "emqx");
                CollectSink<EMQXMessage<String>> sink = new CollectSink<EMQXMessage<String>>();
                source.sinkTo(sink);
                JobClient jobClient = env.executeAsync();

                waitUntilRunning(jobClient);
                // Thread.sleep(1000); // Give more time for MQTT connection to stabilize

                MqttAsyncClient client = startClient(brokerHost, brokerPort);
                String topic = "t/1";
                // Subscribe for debugging
                client.subscribe(topicFilter, qos).waitForCompletion();

                int[] ns = { 1, 2, 3 };
                for (int n : ns) {
                        MqttMessage message = new MqttMessage(String.valueOf(n).getBytes());
                        message.setQos(qos);
                        client.publish(topic, message).waitForCompletion();
                }
                // With Paho auto-ack, we expect at least 3 messages (shared subscription
                // distributes them)
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
                List<Subscription> subscriptions = new ArrayList<>();
                subscriptions.add(new SharedSubscription(groupName, topicFilter, qos));
                StringDeserializer deserializer = new StringDeserializer();

                EMQXSource<String> emqxSource = new EMQXSource<String>(brokerHost, brokerPort, clientid,
                                subscriptions, 
                                SslOption.SSL_DISABLED, null, null, null,
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

                // Uncomment to explore checkpoint; note that final condition does not
                // hold, as the savepoint itself is a checkpoint, and thus ack any pending
                // messages.

                // warnBanner("Restoring checkpoint");
                // final StreamExecutionEnvironment env2 =
                // StreamExecutionEnvironment.getExecutionEnvironment();
                // env2.setParallelism(3);
                // Configuration config = new Configuration();
                // config.set(StateRecoveryOptions.SAVEPOINT_PATH, savepointPath);
                // env2.configure(config);

                // DataStreamSource<EMQXMessage<String>> source2 = env2.fromSource(emqxSource,
                // WatermarkStrategy.noWatermarks(),
                // "emqx");
                // CollectSink<EMQXMessage<String>> sink2 = new
                // CollectSink<EMQXMessage<String>>();
                // source2.sinkTo(sink2);
                // JobClient jobClient2 = env2.executeAsync();
                // CommonTestUtils.waitUntilCondition(() -> sink.getCount() == msgs.size(),
                // 500L, 5);

                // jobClient2.cancel().join();
        }

        @ParameterizedTest(name = "Message QoS = {arguments}")
        // N.B.: At the time of writing, paho mqtt client manual acknowledgement is
        // totally broken for QoS 2, hence we don't test QoS 2 here. When it's fixed,
        // use
        // the following line to test QoS recovery.
        // @ValueSource(ints = { 1, 2 })
        @ValueSource(ints = { 1 })
        public void recoverAfterFailure(int qos) throws Exception {
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);
                String brokerHost = emqx.getHost();
                int brokerPort = emqx.getMappedPort(1883);
                String clientid = mkClientid();
                String groupName = mkGroupName();
                String topicFilter = "t/#";
                List<Subscription> subscriptions = new ArrayList<>();
                subscriptions.add(new SharedSubscription(groupName, topicFilter, qos));
                StringDeserializer deserializer = new StringDeserializer();

                EMQXSource<String> emqxSource = new CrashingTestEMQXSource<String>(brokerHost, brokerPort, clientid,
                                subscriptions,
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
                warnBanner("triggering crash by stopping with savepoint");
                assertThrows(ExecutionException.class,
                                () -> jobClient.stopWithSavepoint(
                                                false,
                                                "/tmp/bah",
                                                SavepointFormatType.CANONICAL)
                                                .get());
                jobClient.cancel().join();

                warnBanner("starting new job");
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
                                String.format("final count (qos %d): %d", qos, sink2.getCount()));

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
                        List<Subscription> subscriptions = new ArrayList<>();
                        subscriptions.add(new SharedSubscription(groupName, topicFilter, qos));
                        StringDeserializer deserializer = new StringDeserializer();

                        EMQXSource<String> emqxSource = new EMQXSource<String>(brokerHost, brokerPort,
                                        clientid,
                                        subscriptions, 
                                        SslOption.SSL_DISABLED, null, null, null,
                                        deserializer);
                        DataStreamSource<EMQXMessage<String>> source = env.fromSource(emqxSource,
                                        WatermarkStrategy.noWatermarks(),
                                        "emqx");
                        CollectSink<EMQXMessage<String>> sink = new CollectSink<EMQXMessage<String>>();
                        source.sinkTo(sink);
                        JobClient jobClient = env.executeAsync();

                        Thread.sleep(2_000L); // Wait longer for reconnection attempts

                        emqx.getDockerClient().unpauseContainerCmd(emqx.getContainerId()).exec();

                        // Thread.sleep(2_000L); // Give time for MQTT to reconnect

                        waitUntilRunning(jobClient);

                        MqttAsyncClient client = startClient(brokerHost, brokerPort);
                        String topic = "t/1";
                        // Subscribe for debugging
                        client.subscribe(topicFilter, qos).waitForCompletion();

                        // Thread.sleep(500); // Let subscription stabilize

                        List<String> msgs = IntStream.range(0, 10).mapToObj(String::valueOf)
                                        .collect(Collectors.toList());
                        for (String msg : msgs) {
                                MqttMessage message = new MqttMessage(msg.getBytes());
                                message.setQos(qos);
                                client.publish(topic, message).waitForCompletion();
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

        @Test
        public void multipleSubscriptionKinds() throws Exception {
                setupEMQXQueue();

                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(3);
                String brokerHost = emqx.getHost();
                int brokerPort = emqx.getMappedPort(1883);
                int qos = 1;
                String clientid = mkClientid();
                String groupName = mkGroupName();
                List<Subscription> subscriptions = new ArrayList<>();
                String sharedSub = "shared/#";
                String queueSub = "queue/#";
                subscriptions.add(new SharedSubscription(groupName, sharedSub, qos));
                subscriptions.add(new QueueSubscription(queueSub, qos));
                // These subscriptions are not repeatable.
                String[] uniqueSubs = { "t/1", "t/2", "t/3" };
                for (String uniqueSub : uniqueSubs) {
                        subscriptions.add(new SimpleSubscription(uniqueSub, qos));
                }
                StringDeserializer deserializer = new StringDeserializer();

                EMQXSource<String> emqxSource = new EMQXSource<String>(brokerHost, brokerPort,
                                clientid,
                                subscriptions, 
                                SslOption.SSL_DISABLED, null, null, null,
                                deserializer);
                DataStreamSource<EMQXMessage<String>> source = env.fromSource(emqxSource,
                                WatermarkStrategy.noWatermarks(),
                                "emqx");
                CollectSink<EMQXMessage<String>> sink = new CollectSink<EMQXMessage<String>>();
                source.sinkTo(sink);
                JobClient jobClient = env.executeAsync();

                waitUntilRunning(jobClient);

                MqttAsyncClient client = startClient(brokerHost, brokerPort);
                // Subscribe for debugging
                client.subscribe(sharedSub, qos).waitForCompletion();
                client.subscribe(queueSub, qos).waitForCompletion();
                client.subscribe("t/#", qos).waitForCompletion();

                // Messages meant for shared subscription
                List<String> msgs1 = IntStream.range(0, 10).mapToObj(String::valueOf).collect(Collectors.toList());
                for (String msg : msgs1) {
                        MqttMessage message = new MqttMessage(msg.getBytes());
                        message.setQos(qos);
                        client.publish(String.format("shared/%s", msg), message).waitForCompletion();
                }
                // Messages meant for queue subscription
                List<String> msgs2 = IntStream.range(10, 20).mapToObj(String::valueOf).collect(Collectors.toList());
                for (String msg : msgs2) {
                        MqttMessage message = new MqttMessage(msg.getBytes());
                        message.setQos(qos);
                        client.publish(String.format("queue/%s", msg), message).waitForCompletion();
                }
                // Messages meant for unique subscriptions
                List<Integer> msgs3 = IntStream.range(20, 30).mapToObj((n) -> n).collect(Collectors.toList());
                for (Integer msg : msgs3) {
                        MqttMessage message = new MqttMessage(String.valueOf(msg).getBytes());
                        message.setQos(qos);
                        String t = String.format("t/%d", 1 + msg % uniqueSubs.length);
                        client.publish(t, message).waitForCompletion();
                }
                int numAllMsgs = msgs1.size() + msgs2.size() + msgs3.size();

                LOG.info("waiting for {} messages to be consumed", numAllMsgs);
                // LOG.info("logs:\n  {}", emqx.getLogs());
                CommonTestUtils.waitUntilCondition(() -> sink.getCount() == numAllMsgs, 500L, 5);

                jobClient.cancel().join();
                client.disconnect().waitForCompletion();
                client.close();
        }
}
