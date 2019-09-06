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
package io.kareldb;

import io.kcache.KafkaCacheConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class KarelDbConfig extends KafkaCacheConfig {

    public static final String LISTENERS_CONFIG = "listeners";
    public static final String LISTENERS_DEFAULT = "http://0.0.0.0:8765";
    public static final String LISTENERS_DOC =
        "List of listeners. http and https are supported. Each listener must include the protocol, "
            + "hostname, and port. For example: http://myhost:8080, https://0.0.0.0:8081";

    public static final String LEADER_ELIGIBILITY_CONFIG = "leader.eligibility";
    public static final boolean LEADER_ELIGIBILITY_DEFAULT = true;
    public static final String LEADER_ELIGIBILITY_DOC =
        "If true, this node can participate in leader election. In a multi-colo setup, turn this off "
            + "for clusters in the replica data center.";

    public static final String ROCKS_DB_ENABLE_CONFIG = "rocksdb.enable";
    public static final boolean ROCKS_DB_ENABLE_DEFAULT = true;
    public static final String ROCKS_DB_ENABLE_DOC =
        "Whether to enable RocksDB within KCache.";

    public static final String ROCKS_DB_ROOT_DIR_CONFIG = "rocksdb.root.dir";
    public static final String ROCKS_DB_ROOT_DIR_DEFAULT = "/tmp";
    public static final String ROCKS_DB_ROOT_DIR_DOC =
        "Root directory for RocksDB storage.";

    private static final ConfigDef config;

    static {
        config = baseConfigDef()
            .define(LISTENERS_CONFIG,
                ConfigDef.Type.LIST,
                LISTENERS_DEFAULT,
                ConfigDef.Importance.HIGH,
                LISTENERS_DOC)
            .define(LEADER_ELIGIBILITY_CONFIG,
                ConfigDef.Type.BOOLEAN,
                LEADER_ELIGIBILITY_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                LEADER_ELIGIBILITY_DOC)
            .define(ROCKS_DB_ENABLE_CONFIG,
                ConfigDef.Type.BOOLEAN,
                ROCKS_DB_ENABLE_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                ROCKS_DB_ENABLE_DOC)
            .define(ROCKS_DB_ROOT_DIR_CONFIG,
                ConfigDef.Type.STRING,
                ROCKS_DB_ROOT_DIR_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                ROCKS_DB_ROOT_DIR_DOC);
    }

    public KarelDbConfig(String propsFile) {
        super(config, getPropsFromFile(propsFile));
    }

    public KarelDbConfig(Map<?, ?> props) {
        super(config, props);
    }
}
