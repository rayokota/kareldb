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
package io.kareldb.server;

import io.kareldb.KarelDbConfig;
import io.kareldb.KarelDbEngine;
import io.kareldb.jdbc.Driver;
import io.kareldb.jdbc.MetaImpl;
import io.kareldb.schema.SchemaFactory;
import io.kareldb.server.handler.DynamicAvaticaJsonHandler;
import io.kareldb.server.leader.KarelDbIdentity;
import io.kareldb.server.leader.KarelDbLeaderElector;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.AvaticaHandler;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.kafka.common.config.ConfigException;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KarelDbMain {
    private static final Logger LOG = LoggerFactory.getLogger(KarelDbMain.class);

    private KarelDbMain() {
    }

    public static void main(String[] args) {
        try {
            if (args.length < 1) {
                LOG.error("Properties file is required to start");
                System.exit(1);
            }
            KarelDbConfig config = new KarelDbConfig(args[0]);
            KarelDbEngine engine = KarelDbEngine.getInstance();
            engine.configure(config);
            engine.init();
            LOG.info("Starting leader election...");
            KarelDbLeaderElector elector = new KarelDbLeaderElector(config, engine);
            elector.init();
            boolean isLeader = elector.isLeader();
            LOG.info("Leader elected, starting server...");
            HttpServer server = start(config, elector);
            LOG.info("Server started, listening for requests...");
            LOG.info("KarelDB is at your service...");
            server.join();
        } catch (Exception e) {
            LOG.error("Server died unexpectedly: ", e);
            System.exit(1);
        }
    }

    public static HttpServer start(KarelDbConfig config, KarelDbLeaderElector elector) throws SQLException {
        KarelDbIdentity identity = elector.getIdentity();
        int port = identity.getPort();
        Meta meta = create(config);
        Service service = new LocalService(meta);
        AvaticaHandler localHandler = new AvaticaJsonHandler(service);
        AvaticaHandler handler = new DynamicAvaticaJsonHandler(config, localHandler, elector);
        HttpServer server = identity.getScheme().equals("http")
            ? new HttpServer(port, handler)
            : new HttpServer(port, handler, null, null, createSslContextFactory(config));
        server.start();
        return server;
    }

    public static Meta create(KarelDbConfig config) {
        try {
            Map<String, String> configs = config.originalsStrings();
            Properties properties = new Properties();
            properties.put(CalciteConnectionProperty.SCHEMA_FACTORY.camelName(), SchemaFactory.class.getName());
            properties.put(CalciteConnectionProperty.SCHEMA.camelName(), "default");
            properties.put(CalciteConnectionProperty.PARSER_FACTORY.camelName(),
                "org.apache.calcite.sql.parser.parserextension.ExtensionSqlParserImpl#FACTORY");
            properties.put("schema.kind", "io.kareldb.kafka.KafkaSchema");
            for (Map.Entry<String, String> entry : configs.entrySet()) {
                properties.put("schema." + entry.getKey(), entry.getValue());
            }

            boolean testMode = false;
            if (testMode) {
                // Single connection for testing
                final Connection connection = DriverManager.getConnection(Driver.CONNECT_STRING_PREFIX, properties);
                return new MetaImpl((AvaticaConnection) connection);
            } else {
                // Multi-connection support
                return new JdbcMeta(Driver.CONNECT_STRING_PREFIX, properties);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private static SslContextFactory createSslContextFactory(KarelDbConfig config) {
        SslContextFactory sslContextFactory = new SslContextFactory();
        if (!config.getString(KarelDbConfig.SSL_KEYSTORE_LOCATION_CONFIG).isEmpty()) {
            sslContextFactory.setKeyStorePath(
                config.getString(KarelDbConfig.SSL_KEYSTORE_LOCATION_CONFIG)
            );
            sslContextFactory.setKeyStorePassword(
                config.getPassword(KarelDbConfig.SSL_KEYSTORE_PASSWORD_CONFIG).value()
            );
            sslContextFactory.setKeyManagerPassword(
                config.getPassword(KarelDbConfig.SSL_KEY_PASSWORD_CONFIG).value()
            );
            sslContextFactory.setKeyStoreType(
                config.getString(KarelDbConfig.SSL_KEYSTORE_TYPE_CONFIG)
            );

            if (!config.getString(KarelDbConfig.SSL_KEYMANAGER_ALGORITHM_CONFIG).isEmpty()) {
                sslContextFactory.setKeyManagerFactoryAlgorithm(
                    config.getString(KarelDbConfig.SSL_KEYMANAGER_ALGORITHM_CONFIG));
            }
        }

        configureClientAuth(config, sslContextFactory);

        List<String> enabledProtocols = config.getList(KarelDbConfig.SSL_ENABLED_PROTOCOLS_CONFIG);
        if (!enabledProtocols.isEmpty()) {
            sslContextFactory.setIncludeProtocols(enabledProtocols.toArray(new String[0]));
        }

        List<String> cipherSuites = config.getList(KarelDbConfig.SSL_CIPHER_SUITES_CONFIG);
        if (!cipherSuites.isEmpty()) {
            sslContextFactory.setIncludeCipherSuites(cipherSuites.toArray(new String[0]));
        }

        sslContextFactory.setEndpointIdentificationAlgorithm(
            config.getString(KarelDbConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG));

        if (!config.getString(KarelDbConfig.SSL_TRUSTSTORE_LOCATION_CONFIG).isEmpty()) {
            sslContextFactory.setTrustStorePath(
                config.getString(KarelDbConfig.SSL_TRUSTSTORE_LOCATION_CONFIG)
            );
            sslContextFactory.setTrustStorePassword(
                config.getPassword(KarelDbConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG).value()
            );
            sslContextFactory.setTrustStoreType(
                config.getString(KarelDbConfig.SSL_TRUSTSTORE_TYPE_CONFIG)
            );

            if (!config.getString(KarelDbConfig.SSL_TRUSTMANAGER_ALGORITHM_CONFIG).isEmpty()) {
                sslContextFactory.setTrustManagerFactoryAlgorithm(
                    config.getString(KarelDbConfig.SSL_TRUSTMANAGER_ALGORITHM_CONFIG)
                );
            }
        }

        sslContextFactory.setProtocol(config.getString(KarelDbConfig.SSL_PROTOCOL_CONFIG));
        if (!config.getString(KarelDbConfig.SSL_PROVIDER_CONFIG).isEmpty()) {
            sslContextFactory.setProtocol(config.getString(KarelDbConfig.SSL_PROVIDER_CONFIG));
        }

        sslContextFactory.setRenegotiationAllowed(false);

        return sslContextFactory;
    }

    @SuppressWarnings("deprecation")
    private static void configureClientAuth(KarelDbConfig config, SslContextFactory sslContextFactory) {
        String clientAuthentication = config.getString(KarelDbConfig.SSL_CLIENT_AUTHENTICATION_CONFIG);

        switch (clientAuthentication) {
            case KarelDbConfig.SSL_CLIENT_AUTHENTICATION_REQUIRED:
                sslContextFactory.setNeedClientAuth(true);
                break;
            case KarelDbConfig.SSL_CLIENT_AUTHENTICATION_REQUESTED:
                sslContextFactory.setWantClientAuth(true);
                break;
            case KarelDbConfig.SSL_CLIENT_AUTHENTICATION_NONE:
                break;
            default:
                throw new ConfigException(
                    "Unexpected value for {} configuration: {}",
                    KarelDbConfig.SSL_CLIENT_AUTHENTICATION_CONFIG,
                    clientAuthentication
                );
        }
    }
}
