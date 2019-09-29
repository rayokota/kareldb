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
import io.kareldb.server.leader.KarelDbIdentity;
import org.apache.calcite.avatica.remote.AuthenticationType;
import org.apache.calcite.avatica.server.AvaticaHandler;
import org.apache.calcite.avatica.server.AvaticaServerConfiguration;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.RemoteUserExtractor;
import org.apache.kafka.common.config.ConfigException;
import org.eclipse.jetty.jaas.JAASLoginService;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;

public class HttpServerExtension extends HttpServer {
    private static final Logger LOG = LoggerFactory.getLogger(HttpServerExtension.class);

    public HttpServerExtension(KarelDbIdentity identity, AvaticaHandler handler, KarelDbConfig config) {
        super(identity.getPort(), handler, buildBasicAuthConfig(config), null,
            identity.getScheme().equals("https") ? createSslContextFactory(config) : null);
    }

    @Override
    protected ConstraintSecurityHandler configureBasicAuthentication(Server server,
                                                                     AvaticaServerConfiguration config) {
        final String[] allowedRoles = config.getAllowedRoles();
        final String realm = config.getHashLoginServiceRealm();

        JAASLoginService loginService = new JAASLoginService(realm);
        server.addBean(loginService);

        return configureCommonAuthentication(Constraint.__BASIC_AUTH,
            allowedRoles, new BasicAuthenticator(), null, loginService);
    }

    private static AvaticaServerConfiguration buildBasicAuthConfig(KarelDbConfig config) {
        String authMethod = config.getString(KarelDbConfig.AUTHENTICATION_METHOD_CONFIG);
        if (!KarelDbConfig.AUTHENTICATION_METHOD_BASIC.equals(authMethod)) {
            return null;
        }

        String realm = config.getString(KarelDbConfig.AUTHENTICATION_REALM_CONFIG);
        List<String> roles = config.getList(KarelDbConfig.AUTHENTICATION_ROLES_CONFIG);
        String[] allowedRoles = roles.toArray(new String[0]);

        return new AvaticaServerConfiguration() {
            @Override
            public AuthenticationType getAuthenticationType() {
                return AuthenticationType.BASIC;
            }

            @Override
            public String[] getAllowedRoles() {
                return allowedRoles;
            }

            @Override
            public String getHashLoginServiceRealm() {
                return realm;
            }

            @Override
            public String getHashLoginServiceProperties() {
                return null;
            }

            @Override
            public String getKerberosRealm() {
                return null;
            }

            @Override
            public String getKerberosPrincipal() {
                return null;
            }

            @Override
            public boolean supportsImpersonation() {
                return false;
            }

            @Override
            public <T> T doAsRemoteUser(String remoteUserName, String remoteAddress,
                                        Callable<T> action) throws Exception {
                return null;
            }

            @Override
            public RemoteUserExtractor getRemoteUserExtractor() {
                return null;
            }
        };
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
