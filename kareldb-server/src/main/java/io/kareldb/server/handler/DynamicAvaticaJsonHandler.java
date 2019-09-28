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
package io.kareldb.server.handler;

import io.kareldb.KarelDbConfig;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.Timer;
import org.apache.calcite.avatica.metrics.Timer.Context;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.remote.Handler.HandlerResponse;
import org.apache.calcite.avatica.remote.JsonHandler;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.remote.Service.RpcMetadataResponse;
import org.apache.calcite.avatica.server.AbstractAvaticaHandler;
import org.apache.calcite.avatica.server.AvaticaHandler;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.server.AvaticaServerConfiguration;
import org.apache.calcite.avatica.server.MetricsAwareAvaticaHandler;
import org.apache.calcite.avatica.server.RemoteUserDisallowedException;
import org.apache.calcite.avatica.server.RemoteUserExtractionException;
import org.apache.calcite.avatica.util.UnsynchronizedBuffer;
import org.eclipse.jetty.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

import static org.apache.calcite.avatica.remote.MetricsHelper.concat;

/**
 * Jetty handler that executes Avatica JSON request-responses.
 */
public class DynamicAvaticaJsonHandler extends AbstractAvaticaHandler {
    private static final Logger LOG = LoggerFactory.getLogger(AvaticaJsonHandler.class);

    private final KarelDbConfig config;
    private final AvaticaHandler localHandler;
    private final UrlProvider urlProvider;
    private final MetricsSystem metrics;
    private final Timer requestTimer;
    private final ThreadLocal<UnsynchronizedBuffer> threadLocalBuffer;
    private final AvaticaServerConfiguration serverConfig;
    private RpcMetadataResponse metadata;

    public DynamicAvaticaJsonHandler(KarelDbConfig config, AvaticaHandler localHandler, UrlProvider urlProvider) {
        this(config, localHandler, urlProvider, NoopMetricsSystem.getInstance(), null);
    }

    public DynamicAvaticaJsonHandler(KarelDbConfig config, AvaticaHandler localHandler, UrlProvider urlProvider,
                                     MetricsSystem metrics) {
        this(config, localHandler, urlProvider, metrics, null);
    }

    public DynamicAvaticaJsonHandler(KarelDbConfig config, AvaticaHandler localHandler, UrlProvider urlProvider,
                                     MetricsSystem metrics,
                                     AvaticaServerConfiguration serverConfig) {
        this.config = config;
        this.localHandler = Objects.requireNonNull(localHandler);
        this.urlProvider = Objects.requireNonNull(urlProvider);
        this.metrics = Objects.requireNonNull(metrics);

        // Metrics
        this.requestTimer = this.metrics.getTimer(
            concat(AvaticaJsonHandler.class, MetricsAwareAvaticaHandler.REQUEST_TIMER_NAME));

        this.threadLocalBuffer = ThreadLocal.withInitial(UnsynchronizedBuffer::new);

        this.serverConfig = serverConfig;
    }

    public void handle(String target, Request baseRequest,
                       HttpServletRequest request, HttpServletResponse response)
        throws IOException, ServletException {

        Optional<String> url = urlProvider.url();
        if (!url.isPresent()) {
            localHandler.handle(target, baseRequest, request, response);
            return;
        }

        AvaticaConnection connection = null;
        try (final Context ctx = requestTimer.start()) {
            connection = (AvaticaConnection) DriverManager.getConnection(getJdbcUrl(url.get()));
            Service service = connection.getService();
            service.setRpcMetadata(metadata);
            JsonHandler jsonHandler = new JsonHandler(service, this.metrics);
            jsonHandler.setRpcMetadata(metadata);

            if (!isUserPermitted(serverConfig, baseRequest, request, response)) {
                LOG.debug("HTTP request from {} is unauthenticated and authentication is required",
                    request.getRemoteAddr());
                return;
            }

            response.setContentType("application/json;charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            if (request.getMethod().equals("POST")) {
                // First look for a request in the header, then look in the body.
                // The latter allows very large requests without hitting HTTP 413.
                String rawRequest = request.getHeader("request");
                if (rawRequest == null) {
                    // Avoid a new buffer creation for every HTTP request
                    final UnsynchronizedBuffer buffer = threadLocalBuffer.get();
                    try (ServletInputStream inputStream = request.getInputStream()) {
                        rawRequest = AvaticaUtils.readFully(inputStream, buffer);
                    } finally {
                        // Reset the offset into the buffer after we're done
                        buffer.reset();
                    }
                }
                final String jsonRequest =
                    new String(rawRequest.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
                LOG.trace("request: {}", jsonRequest);

                HandlerResponse<String> jsonResponse;
                try {
                    if (null != serverConfig && serverConfig.supportsImpersonation()) {
                        String remoteUser = serverConfig.getRemoteUserExtractor().extract(request);
                        jsonResponse = serverConfig.doAsRemoteUser(remoteUser,
                            request.getRemoteAddr(), () -> jsonHandler.apply(jsonRequest));
                    } else {
                        jsonResponse = jsonHandler.apply(jsonRequest);
                    }
                } catch (RemoteUserExtractionException e) {
                    LOG.debug("Failed to extract remote user from request", e);
                    jsonResponse = jsonHandler.unauthenticatedErrorResponse(e);
                } catch (RemoteUserDisallowedException e) {
                    LOG.debug("Remote user is not authorized", e);
                    jsonResponse = jsonHandler.unauthorizedErrorResponse(e);
                } catch (Exception e) {
                    LOG.debug("Error invoking request from {}", baseRequest.getRemoteAddr(), e);
                    jsonResponse = jsonHandler.convertToErrorResponse(e);
                }

                LOG.trace("response: {}", jsonResponse);
                baseRequest.setHandled(true);
                // Set the status code and write out the response.
                response.setStatus(jsonResponse.getStatusCode());
                response.getWriter().println(jsonResponse.getResponse());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void setServerRpcMetadata(RpcMetadataResponse metadata) {
        this.metadata = metadata;
    }

    @Override
    public MetricsSystem getMetrics() {
        return metrics;
    }

    private String getJdbcUrl(String url) {
        String jdbcUrl = "jdbc:avatica:remote:url=" + url + ";serialization=JSON";
        if (url.startsWith("https")) {
            jdbcUrl += ";truststore=" + config.getString(KarelDbConfig.SSL_TRUSTSTORE_LOCATION_CONFIG);
            jdbcUrl += ";truststore_password=" + config.getPassword(KarelDbConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG).value();
        }
        return jdbcUrl;
    }
}
