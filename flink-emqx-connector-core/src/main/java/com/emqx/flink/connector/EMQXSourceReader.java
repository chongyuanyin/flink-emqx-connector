package com.emqx.flink.connector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import java.security.cert.Certificate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.runtime.io.MultipleFuturesAvailabilityHelper;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.pqc.jcajce.provider.BouncyCastlePQCProvider;

public class EMQXSourceReader<OUT> implements SourceReader<EMQXMessage<OUT>, EMQXSourceSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(EMQXSourceReader.class);
    private static final String TLS_PROTOCOL = "TLSv1.2";

    // {qos, messageId, msg}
    private Queue<Tuple3<Integer, Integer, EMQXMessage<OUT>>> queue = new ConcurrentLinkedQueue<>();
    private MqttAsyncClient client;
    private MultipleFuturesAvailabilityHelper availabilityHelper = new MultipleFuturesAvailabilityHelper(1);

    private SourceReaderContext context;
    private String brokerHost;
    private int brokerPort;
    private String clientid;
    private String username;
    private String password;
    private SslOption sslOption;
    private String brokerCaFile;
    private String connectorCrtFile;
    private String connectorKeyFile;
    private DeserializationSchema<OUT> deserializer;
    private List<EMQXSourceSplit> splits = new ArrayList<>();
    private List<EMQXSourceSplit> pendingSplits = new ArrayList<>();
    // list of received message ids pending ack
    private final List<Tuple2<Integer, Integer>> msgsToAck = new ArrayList<>();
    private final SortedMap<Long, List<Tuple2<Integer, Integer>>> checkpointsToMsgsToAck;

    EMQXSourceReader(
            SourceReaderContext context, String brokerHost, int brokerPort, String clientid,
            String username, String password, SslOption sslOption, String brokerCaFile, 
            String connectorCrtFile, String connectorKeyFile, DeserializationSchema<OUT> deserializer) {
        this.context = context;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.clientid = clientid;
        this.username = username;
        this.password = password;
        this.sslOption = sslOption;
        this.brokerCaFile = brokerCaFile;
        this.connectorCrtFile = connectorCrtFile;
        this.connectorKeyFile = connectorKeyFile;
        this.deserializer = deserializer;
        this.checkpointsToMsgsToAck = Collections.synchronizedSortedMap(new TreeMap<>());
    }

    void consumeMessage(String topic, MqttMessage message) {
        LOG.debug("received message on topic: {}", topic);
        try {
            OUT decoded = deserializer.deserialize(message.getPayload());
            EMQXMessage<OUT> emqxMessage = new EMQXMessage<>(
                    topic,
                    message.getQos(),
                    message.isRetained(),
                    message.getProperties(),
                    decoded);

            queue.add(new Tuple3<>(message.getQos(), message.getId(), emqxMessage));

            CompletableFuture<Void> cachedPreviousFuture = (CompletableFuture<Void>) availabilityHelper
                    .getAvailableFuture();
            cachedPreviousFuture.complete(null);
        } catch (IOException e) {
            LOG.error("error deserializing mqtt message", e);
        }
    }

    MqttAsyncClient startClient(String host, int port, String clientid, String username, String password,
            DeserializationSchema<OUT> deserializer) throws Exception {

        String schema = "tcp";
        if (sslOption == SslOption.SSL_VERIFY_NONE || sslOption == SslOption.SSL_VERIFY_PEER) {
            schema = "ssl";
        }
        String broker = String.format("%s://%s:%d", schema, host, port);
        MqttAsyncClient client = new MqttAsyncClient(broker, clientid);

        // Set callback for incoming messages
        client.setCallback(new MqttCallback() {
            @Override
            public void disconnected(MqttDisconnectResponse disconnectResponse) {
                LOG.warn("Client {} disconnected: {}", clientid, disconnectResponse);
            }

            @Override
            public void mqttErrorOccurred(MqttException exception) {
                LOG.error("MQTT error occurred for client {}", clientid, exception);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                consumeMessage(topic, message);
            }

            @Override
            public void deliveryComplete(IMqttToken token) {
                // Not used for consumer
            }

            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                LOG.info("Connect complete for client {}. Reconnect: {}", clientid, reconnect);
            }

            @Override
            public void authPacketArrived(int reasonCode, MqttProperties properties) {
                // Not used
            }
        });

        client.setManualAcks(true);

        // TLS support
        SSLSocketFactory sslFactory = null;
        KeyManager[] keyManagers = null;
        TrustManager[] trustManagers = null;
        try {
            if (sslOption == SslOption.SSL_VERIFY_NONE || sslOption == SslOption.SSL_VERIFY_PEER) {
                Security.addProvider(new BouncyCastleProvider());
                // connector should send its cert (with key) to broker for verification
                KeyStore keyStore = KeyStore.getInstance("PKCS12", "BC");
                keyStore.load(null, null);
    
                PrivateKey privateKey = SSLUtil.getPrivateKey(new File(connectorKeyFile));
                List<Certificate> certChain = SSLUtil.getCertChain(new File(connectorCrtFile));
    
                keyStore.setKeyEntry("connector-private-key", privateKey, SSLUtil.getDefaultKeyStorePassword(),
                    certChain.toArray(new Certificate[0]));
                
                KeyManagerFactory kmFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmFactory.init(keyStore, SSLUtil.getDefaultKeyStorePassword());
                keyManagers = kmFactory.getKeyManagers();
    
                if (sslOption == SslOption.SSL_VERIFY_PEER) {
                    // connector should verify broker's identity
                    if (brokerCaFile != null) {
                        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
                        try (InputStream is = new FileInputStream(brokerCaFile)) {
                            trustStore.load(is, null);
                        }
                        TrustManagerFactory tmFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                        tmFactory.init(trustStore);
                        trustManagers = tmFactory.getTrustManagers();
                    } else {
                        throw new Exception("Broker CA file is not provided");
                    }
                } else {
                    // connector trust any broker's identity
                    trustManagers = new TrustManager[]{
                        new X509TrustManager() {
                            @Override
                            public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                                return;
                            }
                            
                            @Override
                            public void checkClientTrusted(X509Certificate[] chain, String authType)
                                    throws CertificateException {
                                return;
                            }
                            @Override
                            public X509Certificate[] getAcceptedIssuers() {
                                return new X509Certificate[0];
                            }
                        }
                    };
                }

                SSLContext sslContext = SSLContext.getInstance(TLS_PROTOCOL);
                sslContext.init(keyManagers, trustManagers, null);
                sslFactory = sslContext.getSocketFactory();
            }            
        } catch(Exception e) {
            throw e;
        }
        
        // if (tlsEnabled && caFile != null) {
        //     try {
        //         KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        //         try (InputStream is = new FileInputStream(caFile)) {
        //             trustStore.load(is, null);
        //         }
    
        //         TrustManagerFactory tmFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        //         tmFactory.init(trustStore);
    
        //         SSLContext sslContext = SSLContext.getInstance(TLS_PROTOCOL);
        //         sslContext.init(null, tmFactory.getTrustManagers(), null);
        //         sslFactory = sslContext.getSocketFactory();
        //     } 
        // }

        // Configure connection options
        MqttConnectionOptions options = new MqttConnectionOptions();
        options.setCleanStart(false);
        options.setSessionExpiryInterval(60L);
        options.setAutomaticReconnect(true);
        if (sslFactory != null) {
            options.setSocketFactory(sslFactory);
        }

        // Only set auth if username and password are provided
        if (username != null && !username.isEmpty() && password != null && !password.isEmpty()) {
            options.setUserName(username);
            options.setPassword(password.getBytes());
        }

        LOG.info("Connecting MQTT client for {}", clientid);

        // Connect and subscribe
        // TODO: must schedule a retry if this fails, since automatic reconnect only
        // works after initial connection...
        client.connect(options).waitForCompletion();
        LOG.info("MQTT client for {} connected", clientid);

        // Subscribe to topics
        // TODO: should check CONNACK for session presence and skip subscribing if present.
        Iterator<EMQXSourceSplit> iter = pendingSplits.iterator();
        EMQXSourceSplit split;
        while (iter.hasNext()) {
            split = iter.next();
            if (subscribeToTopic(client, split.topic, split.qos)) {
                iter.remove();
            } else {
                // TODO: schedule retry
            }
        }

        return client;
    }

    private boolean subscribeToTopic(MqttAsyncClient client, String topicFilter, int qos) {
        LOG.info("Subscribing to topic filter {}", topicFilter);
        // do we need to check for success (suback with RC success)?
        try {
            client.subscribe(topicFilter, qos).waitForCompletion();
            LOG.info("Subscribed to topic filter {}", topicFilter);
            return true;
        } catch (MqttException e) {
            LOG.error("client {} failed to subscribe to {} (qos {})", clientid, topicFilter, qos, e);
            return false;
        }
    }

    @Override
    public void start() {
        LOG.debug("starting client {}; splits: {}", clientid, splits);
        if (splits.size() == 0) {
            // Starting for the first time, or recovering without ever having received a
            // split.
            LOG.info("client {} has no splits; requesting", clientid);
            context.sendSplitRequest();
        }

        try {
            client = startClient(brokerHost, brokerPort, clientid, username, password, deserializer);
        } catch (Exception e) {
            LOG.error("Error starting client {}", clientid, e);
            // TODO: schedule retry
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Stopping client");
        if (client != null && client.isConnected()) {
            client.disconnect().waitForCompletion();
            client.close();
        }
    }

    @Override
    public void addSplits(List<EMQXSourceSplit> splits) {
        // Called before `start()` if restoring state, after if we are starting for the
        // first time.
        LOG.info("Adding splits for clientid {}; splits: {}", clientid, splits);
        this.splits.addAll(splits);
        this.pendingSplits.addAll(splits);
        // TODO: handle case when reader is started but client is not connected when we
        // attempt to add subs.
        if (client != null && client.isConnected()) {
            Iterator<EMQXSourceSplit> iter = this.pendingSplits.iterator();
            EMQXSourceSplit split;
            while (iter.hasNext()) {
                split = iter.next();
                if (subscribeToTopic(client, split.topic, split.qos)) {
                    iter.remove();
                } else {
                    // TODO: schedule retry
                }
            }
        }
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        availabilityHelper.resetToUnAvailable();
        return (CompletableFuture<Void>) availabilityHelper.getAvailableFuture();
    }

    @Override
    public void notifyNoMoreSplits() {
    }

    @Override
    public InputStatus pollNext(ReaderOutput<EMQXMessage<OUT>> output) throws Exception {
        Tuple3<Integer, Integer, EMQXMessage<OUT>> tuple = queue.poll();
        if (tuple == null) {
            return InputStatus.NOTHING_AVAILABLE;
        } else {
            output.collect(tuple.f2);
            // QoS 0 does not need ack
            if (tuple.f0 > 0) {
                msgsToAck.add(new Tuple2<>(tuple.f0, tuple.f1));
            }
            return InputStatus.MORE_AVAILABLE;
        }
    }

    @Override
    public List<EMQXSourceSplit> snapshotState(long checkpointId) {
        LOG.debug("snapshotState: checkpointId: {}; splits: {}; msgs to ack: {}", checkpointId, splits, msgsToAck);
        List<Tuple2<Integer, Integer>> msgsToAckTmp = new ArrayList<>(this.msgsToAck);
        synchronized (checkpointsToMsgsToAck) {
            if (msgsToAckTmp.size() > 0) {
                checkpointsToMsgsToAck.put(checkpointId, msgsToAckTmp);
            }
        }
        msgsToAck.clear();
        return splits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.debug("checkpoint complete: {}", checkpointId);
        // Subsume previous checkpoints.
        synchronized (checkpointsToMsgsToAck) {
            if (checkpointsToMsgsToAck.size() > 0) {
                SortedMap<Long, List<Tuple2<Integer, Integer>>> sm = checkpointsToMsgsToAck.subMap(
                        checkpointsToMsgsToAck.firstKey(),
                        // need to guard against overflow?
                        checkpointId + 1);
                // note: assuming messages are enqueued in message id order...
                Iterator<Map.Entry<Long, List<Tuple2<Integer, Integer>>>> iter = sm.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<Long, List<Tuple2<Integer, Integer>>> entry = iter.next();
                    LOG.debug("acking {} messages for checkpoint {}", entry.getValue().size(), entry.getKey());
                    entry.getValue().forEach(tup -> {
                        try {
                            client.messageArrivedComplete(tup.f1, tup.f0);
                        } catch (MqttException e) {
                            LOG.error("failed to ack message id {} (qos {})", tup.f1, tup.f0);
                        }
                    });
                    iter.remove();
                }
            }
        }
        SourceReader.super.notifyCheckpointComplete(checkpointId);
    }
}
