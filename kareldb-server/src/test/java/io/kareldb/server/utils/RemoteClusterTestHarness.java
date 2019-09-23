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

package io.kareldb.server.utils;

import com.google.common.io.Files;
import io.kareldb.KarelDbConfig;
import io.kareldb.KarelDbEngine;
import io.kareldb.server.KarelDbMain;
import io.kareldb.server.leader.KarelDbLeaderElector;
import io.kareldb.utils.ClusterTestHarness;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Properties;

/**
 * Test harness to run against a real, local Kafka cluster. This is essentially
 * Kafka's ZookeeperTestHarness and KafkaServerTestHarness traits combined.
 */
public abstract class RemoteClusterTestHarness extends ClusterTestHarness {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteClusterTestHarness.class);

    private File tempDir;
    protected Integer serverPort;
    protected HttpServer server;


    public RemoteClusterTestHarness() {
        super();
    }

    public RemoteClusterTestHarness(int numBrokers) {
        super(numBrokers);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        tempDir = Files.createTempDir();
        setUpServer();
    }

    private void setUpServer() {
        try {
            Properties props = new Properties();
            serverPort = choosePort();
            props.put(KarelDbConfig.LISTENERS_CONFIG, "http://0.0.0.0:" + serverPort);
            props.put(KarelDbConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(KarelDbConfig.ROCKS_DB_ROOT_DIR_CONFIG, tempDir.getAbsolutePath());

            KarelDbConfig config = new KarelDbConfig(props);
            KarelDbEngine engine = KarelDbEngine.getInstance();
            engine.configure(config);
            engine.init();
            LOG.info("Starting leader election...");
            KarelDbLeaderElector elector = new KarelDbLeaderElector(config, engine);
            elector.init();
            boolean isLeader = elector.isLeader();
            LOG.info("Leader elected, starting server...");
            server = KarelDbMain.start(config, elector);
            LOG.info("Server started, listening for requests...");
            LOG.info("KarelDB is at your service...");
        } catch (Exception e) {
            LOG.error("Server died unexpectedly: ", e);
            System.exit(1);
        }
    }

    /**
     * Choose a number of random available ports
     */
    public static int[] choosePorts(int count) {
        try {
            ServerSocket[] sockets = new ServerSocket[count];
            int[] ports = new int[count];
            for (int i = 0; i < count; i++) {
                sockets[i] = new ServerSocket(0, 0, InetAddress.getByName("0.0.0.0"));
                ports[i] = sockets[i].getLocalPort();
            }
            for (int i = 0; i < count; i++) {
                sockets[i].close();
            }
            return ports;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Choose an available port
     */
    public static int choosePort() {
        return choosePorts(1)[0];
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
        KarelDbEngine.closeInstance();
        FileUtils.deleteDirectory(tempDir);
        super.tearDown();
    }
}
