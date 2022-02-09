/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kareldb.utils;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.CoreUtils;
import kafka.utils.TestUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.junit.After;
import org.junit.Before;
import scala.Option;
import scala.Option$;

import java.util.List;
import java.util.Properties;
import java.util.Vector;

/**
 * Test harness to run against a real, local Kafka cluster. This is essentially
 * Kafka's ZookeeperTestHarness and KafkaServerTestHarness traits combined.
 */
public abstract class ClusterTestHarness {

    protected static final int DEFAULT_NUM_BROKERS = 1;
    protected static final Option<Properties> EMPTY_SASL_PROPERTIES = Option$.MODULE$.<Properties>empty();

    private final int numBrokers;

    // ZK Config
    protected EmbeddedZookeeper zookeeper;
    protected String zkConnect;

    // Kafka Config
    protected List<KafkaConfig> configs = null;
    protected List<KafkaServer> servers = null;
    protected String bootstrapServers = null;

    public ClusterTestHarness() {
        this(DEFAULT_NUM_BROKERS);
    }

    public ClusterTestHarness(int numBrokers) {
        this.numBrokers = numBrokers;
    }

    @Before
    public void setUp() throws Exception {
        zookeeper = new EmbeddedZookeeper();
        zkConnect = String.format("localhost:%d", zookeeper.port());

        configs = new Vector<>();
        servers = new Vector<>();
        for (int i = 0; i < numBrokers; i++) {
            KafkaConfig config = getKafkaConfig(i);
            configs.add(config);

            KafkaServer server = TestUtils.createServer(config, Time.SYSTEM);
            servers.add(server);
        }

        String[] serverUrls = new String[servers.size()];
        ListenerName listenerType = ListenerName.forSecurityProtocol(getSecurityProtocol());
        for (int i = 0; i < servers.size(); i++) {
            serverUrls[i] =
                Utils.formatAddress(
                    servers.get(i).config().effectieAdvertisedListeners().head().host(),
                    servers.get(i).boundPort(listenerType)
                );
        }
        bootstrapServers = Utils.join(serverUrls, ",");
    }

    protected void injectProperties(Properties props) {
        props.setProperty("auto.create.topics.enable", "true");
        props.setProperty("num.partitions", "1");
    }

    protected KafkaConfig getKafkaConfig(int brokerId) {

        final Option<java.io.File> noFile = scala.Option.apply(null);
        final Option<SecurityProtocol> noInterBrokerSecurityProtocol = scala.Option.apply(null);
        Properties props = TestUtils.createBrokerConfig(
            brokerId,
            zkConnect,
            false,
            false,
            TestUtils.RandomPort(),
            noInterBrokerSecurityProtocol,
            noFile,
            EMPTY_SASL_PROPERTIES,
            true,
            false,
            TestUtils.RandomPort(),
            false,
            TestUtils.RandomPort(),
            false,
            TestUtils.RandomPort(),
            Option.<String>empty(),
            1,
            false,
            1,
            (short) 1
        );
        injectProperties(props);
        return KafkaConfig.fromProps(props);
    }

    protected SecurityProtocol getSecurityProtocol() {
        return SecurityProtocol.PLAINTEXT;
    }

    @After
    public void tearDown() throws Exception {
        if (servers != null) {
            for (KafkaServer server : servers) {
                server.shutdown();
            }

            // Remove any persistent data
            for (KafkaServer server : servers) {
                CoreUtils.delete(server.config().logDirs());
            }
        }

        if (zookeeper != null) {
            zookeeper.shutdown();
        }
    }
}
