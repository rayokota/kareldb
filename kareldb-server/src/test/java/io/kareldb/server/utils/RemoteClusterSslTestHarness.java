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

import io.kareldb.KarelDbConfig;
import org.apache.kafka.common.config.types.Password;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;
import java.io.File;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Test harness to run against a real, local Kafka cluster. This is essentially
 * Kafka's ZookeeperTestHarness and KafkaServerTestHarness traits combined.
 */
public abstract class RemoteClusterSslTestHarness extends RemoteClusterTestHarness {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteClusterSslTestHarness.class);

    public RemoteClusterSslTestHarness() {
        super();
    }

    public RemoteClusterSslTestHarness(int numBrokers) {
        super(numBrokers);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void injectKarelDbProperties(Properties props) {
        super.injectKarelDbProperties(props);
        props.put(KarelDbConfig.LISTENERS_CONFIG, "https://localhost:" + serverPort);

        Configuration.setConfiguration(null);
        props.put(KarelDbConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        try {
            File trustStoreFile = File.createTempFile("truststore", ".jks");
            trustStoreFile.deleteOnExit();
            List<X509Certificate> clientCerts = new ArrayList<>();

            List<KeyPair> keyPairs = new ArrayList<>();
            props.putAll(
                SecureTestUtils.clientSslConfigsWithKeyStore(1, trustStoreFile, new Password
                        ("TrustPassword"), clientCerts,
                    keyPairs
                ));
            // Currently REQUIRED cannot be used as client does not use keystore
            props.put(KarelDbConfig.SSL_CLIENT_AUTHENTICATION_CONFIG, KarelDbConfig.SSL_CLIENT_AUTHENTICATION_REQUESTED);

        } catch (Exception e) {
            throw new RuntimeException("Exception creation SSL properties ", e);
        }
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }
}
