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
package io.kareldb.jdbc;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteFactory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.function.Function0;

import java.util.Properties;

/**
 * KarelDB JDBC driver.
 */
public class Driver extends org.apache.calcite.jdbc.Driver {
    public static final String CONNECT_STRING_PREFIX = "jdbc:kareldb:";

    static {
        new Driver().register();
    }

    public Driver() {
        super(io.kareldb.jdbc.CalcitePrepareImpl::new);
    }

    @Override
    protected String getConnectStringPrefix() {
        return CONNECT_STRING_PREFIX;
    }

    protected DriverVersion createDriverVersion() {
        return DriverVersion.load(
            Driver.class,
            "io-kareldb-jdbc.properties",
            "KarelDB JDBC Driver",
            "unknown version",
            "KarelDB",
            "unknown version");
    }

    @Override
    public Meta createMeta(AvaticaConnection connection) {
        return new MetaImpl(connection);
    }

    /**
     * Creates an internal connection.
     */
    CalciteConnection connect(CalciteSchema rootSchema,
                              JavaTypeFactory typeFactory) {
        return (CalciteConnection) ((CalciteFactory) factory)
            .newConnection(this, factory, CONNECT_STRING_PREFIX, new Properties(),
                rootSchema, typeFactory);
    }

    /**
     * Creates an internal connection.
     */
    CalciteConnection connect(CalciteSchema rootSchema,
                              JavaTypeFactory typeFactory, Properties properties) {
        return (CalciteConnection) ((CalciteFactory) factory)
            .newConnection(this, factory, CONNECT_STRING_PREFIX, properties,
                rootSchema, typeFactory);
    }
}
