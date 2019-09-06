/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kareldb.server.leader;

import io.kareldb.KarelDbConfig;
import io.kareldb.server.handler.UrlProvider;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class KarelDbLeaderElector implements KarelDbRebalanceListener, UrlProvider, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KarelDbLeaderElector.class);

    private static final AtomicInteger KDB_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
    private static final String JMX_PREFIX = "kareldb";

    private final int initTimeout;
    private final String clientId;
    private final ConsumerNetworkClient client;
    private final Metrics metrics;
    private final Metadata metadata;
    private final long retryBackoffMs;
    private final KarelDbCoordinator coordinator;
    private KarelDbIdentity myIdentity;
    private volatile KarelDbIdentity leader;

    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private ExecutorService executor;
    private final CountDownLatch joinedLatch = new CountDownLatch(1);

    public KarelDbLeaderElector(KarelDbConfig config) throws KarelDbElectionException {
        try {
            clientId = "kdb-" + KDB_CLIENT_ID_SEQUENCE.getAndIncrement();

            this.myIdentity = findIdentity(
                config.getList(KarelDbConfig.LISTENERS_CONFIG),
                config.getBoolean(KarelDbConfig.LEADER_ELIGIBILITY_CONFIG));

            Map<String, String> metricsTags = new LinkedHashMap<>();
            metricsTags.put("client-id", clientId);
            MetricConfig metricConfig = new MetricConfig().tags(metricsTags);
            List<MetricsReporter> reporters = Collections.singletonList(new JmxReporter(JMX_PREFIX));
            Time time = Time.SYSTEM;

            ClientConfig clientConfig = new ClientConfig(config.originalsWithPrefix("kafkastore."), false);

            this.metrics = new Metrics(metricConfig, reporters, time);
            this.retryBackoffMs = clientConfig.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG);
            String groupId = config.getString(KarelDbConfig.KAFKACACHE_GROUP_ID_CONFIG);
            LogContext logContext = new LogContext("[KarelDB clientId=" + clientId + ", groupId="
                + groupId + "] ");
            this.metadata = new Metadata(
                retryBackoffMs,
                clientConfig.getLong(CommonClientConfigs.METADATA_MAX_AGE_CONFIG),
                logContext,
                new ClusterResourceListeners()
            );
            List<String> bootstrapServers
                = config.getList(KarelDbConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG);
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(bootstrapServers,
                ClientDnsLookup.DEFAULT.name());
            this.metadata.bootstrap(addresses, time.milliseconds());
            String metricGrpPrefix = "kareldb";

            ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(clientConfig, time);
            long maxIdleMs = clientConfig.getLong(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG);

            NetworkClient netClient = new NetworkClient(
                new Selector(maxIdleMs, metrics, time, metricGrpPrefix, channelBuilder, logContext),
                this.metadata,
                clientId,
                100, // a fixed large enough value will suffice
                clientConfig.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG),
                clientConfig.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                clientConfig.getInt(CommonClientConfigs.SEND_BUFFER_CONFIG),
                clientConfig.getInt(CommonClientConfigs.RECEIVE_BUFFER_CONFIG),
                clientConfig.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
                ClientDnsLookup.DEFAULT,
                time,
                true,
                new ApiVersions(),
                logContext);

            this.client = new ConsumerNetworkClient(
                logContext,
                netClient,
                metadata,
                time,
                retryBackoffMs,
                clientConfig.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
                Integer.MAX_VALUE
            );
            this.coordinator = new KarelDbCoordinator(
                logContext,
                this.client,
                groupId,
                300000, // Default MAX_POLL_INTERVAL_MS_CONFIG
                10000, // Default SESSION_TIMEOUT_MS_CONFIG)
                3000, // Default HEARTBEAT_INTERVAL_MS_CONFIG
                metrics,
                metricGrpPrefix,
                time,
                retryBackoffMs,
                myIdentity,
                this
            );

            AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds());

            initTimeout = config.getInt(KarelDbConfig.KAFKACACHE_INIT_TIMEOUT_CONFIG);

            LOG.debug("Group member created");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed
            // this is to prevent resource leak. see KAFKA-2121
            stop(true);
            // now propagate the exception
            throw new KarelDbElectionException("Failed to construct kafka consumer", t);
        }
    }

    static KarelDbIdentity findIdentity(List<String> configuredListeners, boolean leaderEligibility) {
        List<URI> listeners = parseListeners(configuredListeners);
        for (URI listener : listeners) {
            return new KarelDbIdentity(listener.getScheme(), listener.getHost(), listener.getPort(), leaderEligibility);
        }
        throw new ConfigException("No listeners are configured. Must have at least one listener.");
    }

    static List<URI> parseListeners(List<String> listenersConfig) {
        List<URI> listeners = new ArrayList<>(listenersConfig.size());
        for (String listenerStr : listenersConfig) {
            URI uri;
            try {
                uri = new URI(listenerStr);
            } catch (URISyntaxException use) {
                throw new ConfigException(
                    "Could not parse a listener URI from the `listener` configuration option."
                );
            }
            String scheme = uri.getScheme();
            if (scheme == null) {
                throw new ConfigException(
                    "Found a listener without a scheme. All listeners must have a scheme. The "
                        + "listener without a scheme is: " + listenerStr
                );
            }
            if (uri.getPort() == -1) {
                throw new ConfigException(
                    "Found a listener without a port. All listeners must have a port. The "
                        + "listener without a port is: " + listenerStr
                );
            }
            listeners.add(uri);
        }

        if (listeners.isEmpty()) {
            throw new ConfigException("No listeners are configured. Must have at least one listener.");
        }

        return listeners;
    }

    public void init() throws KarelDbElectionException {
        LOG.debug("Initializing group member");

        executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            try {
                while (!stopped.get()) {
                    coordinator.poll(Integer.MAX_VALUE);
                }
            } catch (Throwable t) {
                LOG.error("Unexpected exception in group processing thread", t);
            }
        });

        try {
            if (!joinedLatch.await(initTimeout, TimeUnit.MILLISECONDS)) {
                throw new KarelDbElectionException("Timed out waiting for join group to complete");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KarelDbElectionException("Interrupted while waiting for join group to "
                + "complete", e);
        }

        LOG.debug("Group member initialized and joined group");
    }

    @Override
    public Optional<String> url() {
        return isLeader() ? Optional.empty() : Optional.of(leader.getUrl());
    }

    @Override
    public void close() {
        if (stopped.get()) {
            return;
        }
        stop(false);
    }

    @Override
    public void onAssigned(KarelDbProtocol.Assignment assignment, int generation) {
        LOG.info("Finished rebalance with leader election result: {}", assignment);
        try {
            switch (assignment.error()) {
                case KarelDbProtocol.Assignment.NO_ERROR:
                    if (assignment.leaderIdentity() == null) {
                        LOG.error(
                            "No leader eligible instances joined the group. "
                                + "Rebalancing was successful and this instance can serve reads, but no writes "
                                + "can be processed."
                        );
                    }
                    setLeader(assignment.leaderIdentity());
                    LOG.info(isLeader() ? "Registered as leader" : "Registered as replica");
                    joinedLatch.countDown();
                    break;
                case KarelDbProtocol.Assignment.DUPLICATE_URLS:
                    throw new IllegalStateException(
                        "The group contained multiple members advertising the same URL. "
                            + "Verify that each instance has a unique, routable listener by setting the "
                            + "'listeners' configuration. This error may happen if executing in containers "
                            + "where the default hostname is 'localhost'."
                    );
                default:
                    throw new IllegalStateException("Unknown error returned from the coordination protocol");
            }
        } catch (KarelDbElectionException e) {
            // This shouldn't be possible with this implementation. The exceptions from setLeader come
            // from it calling nextRange in this class, but this implementation doesn't require doing
            // any IO, so the errors that can occur in the ZK implementation should not be possible here.
            LOG.error(
                "Error when updating leader, we will not be able to forward requests to the leader",
                e
            );
        }
    }

    @Override
    public void onRevoked() {
        LOG.info("Rebalance started");
        try {
            setLeader(null);
        } catch (KarelDbElectionException e) {
            // This shouldn't be possible with this implementation. The exceptions from setLeader come
            // from it calling nextRange in this class, but this implementation doesn't require doing
            // any IO, so the errors that can occur in the ZK implementation should not be possible here.
            LOG.error(
                "Error when updating leader, we will not be able to forward requests to the leader",
                e
            );
        }
    }

    public KarelDbIdentity getIdentity() {
        return myIdentity;
    }

    public boolean isLeader() {
        return leader != null && leader.equals(myIdentity);
    }

    private void setLeader(KarelDbIdentity leader) {
        this.leader = leader;
    }

    private void stop(boolean swallowException) {
        LOG.trace("Stopping the group member.");

        // Interrupt any outstanding poll calls
        if (client != null) {
            client.wakeup();
        }

        // Wait for processing thread to complete
        if (executor != null) {
            executor.shutdown();
            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(
                    "Interrupted waiting for group processing thread to exit",
                    e
                );
            }
        }

        // Do final cleanup
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        this.stopped.set(true);
        closeQuietly(coordinator, "coordinator", firstException);
        closeQuietly(metrics, "consumer metrics", firstException);
        closeQuietly(client, "consumer network client", firstException);
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);
        if (firstException.get() != null && !swallowException) {
            throw new KafkaException(
                "Failed to stop the group member",
                firstException.get()
            );
        } else {
            LOG.debug("The group member has stopped.");
        }
    }

    private static void closeQuietly(AutoCloseable closeable,
                                     String name,
                                     AtomicReference<Throwable> firstException
    ) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Throwable t) {
                firstException.compareAndSet(null, t);
                LOG.error("Failed to close {} with type {}", name, closeable.getClass().getName(), t);
            }
        }
    }
}
