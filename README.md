# KarelDB - A Relational Database Backed by Apache Kafka

[![Build Status][travis-shield]][travis-link]
[![Maven][maven-shield]][maven-link]
[![Javadoc][javadoc-shield]][javadoc-link]

[travis-shield]: https://travis-ci.org/rayokota/kareldb.svg?branch=master
[travis-link]: https://travis-ci.org/rayokota/kareldb
[maven-shield]: https://img.shields.io/maven-central/v/io.kareldb/kareldb-core.svg
[maven-link]: https://search.maven.org/#search%7Cga%7C1%7Cio.kareldb
[javadoc-shield]: https://javadoc.io/badge/io.kareldb/kareldb-core.svg?color=blue
[javadoc-link]: https://javadoc.io/doc/io.kareldb/kareldb-core

KarelDB is a fully-functional relational database backed by Apache Kafka.

## Maven

Releases of KarelDB are deployed to Maven Central.

```xml
<dependency>
    <groupId>io.kareldb</groupId>
    <artifactId>kareldb-core</artifactId>
    <version>0.1.6</version>
</dependency>
```

## Server Mode

To run KarelDB, download a [release](https://github.com/rayokota/kareldb/releases), unpack it, and then modify `config/kareldb.properties` to point to an existing Kafka broker.  Then run the following:

```bash
$ bin/kareldb-start config/kareldb.properties
```

At a separate terminal, enter the following command to start up `sqlline`, a command-line utility for accessing JDBC databases.

```
$ bin/sqlline
sqlline version 1.8.0

sqlline> !connect jdbc:avatica:remote:url=http://localhost:8765 admin admin

sqlline> create table books (id int, name varchar, author varchar);
No rows affected (0.114 seconds)

sqlline> insert into books values (1, 'The Trial', 'Franz Kafka');
1 row affected (0.576 seconds)

sqlline> select * from books;
+----+-----------+-------------+
| ID |   NAME    |   AUTHOR    |
+----+-----------+-------------+
| 1  | The Trial | Franz Kafka |
+----+-----------+-------------+
1 row selected (0.133 seconds)
```

To access a KarelDB server from a remote application, use an Avatica JDBC client.  A list of Avatica JDBC clients can be found [here](https://calcite.apache.org/avatica/docs/).

If multiple KarelDB servers are configured with the same cluster group ID (see [Configuration](#configuration)), then they will form a cluster and one of them will be elected as leader, while the others will become followers (replicas).  If a follower receives a request, it will be forwarded to the leader.  If the leader fails, one of the followers will be elected as the new leader.

## Embedded Mode

KarelDB can also be used in embedded mode.  Here is an example:

```java
Properties properties = new Properties();
properties.put("schemaFactory", "io.kareldb.schema.SchemaFactory");
properties.put("parserFactory", "org.apache.calcite.sql.parser.parserextension.ExtensionSqlParserImpl#FACTORY");
properties.put("schema.kind", "io.kareldb.kafka.KafkaSchema");
properties.put("schema.kafkacache.bootstrap.servers", bootstrapServers);
properties.put("schema.rocksdb.root.dir", "/tmp");

try (Connection conn = DriverManager.getConnection("jdbc:kareldb:", properties);
     Statement s = conn.createStatement()) {
        s.execute("create table books (id int, name varchar, author varchar)");
        s.executeUpdate("insert into books values(1, 'The Trial', 'Franz Kafka')");
        ResultSet rs = s.executeQuery("select * from books");
        ...
}
```

## ANSI SQL Support

KarelDB supports ANSI SQL, using [Calcite](https://calcite.apache.org/docs/reference.html).  

When creating a table, the primary key constraint should be specified after the columns, like so:

```
CREATE TABLE customers 
    (id int, name varchar, constraint pk primary key (id));
```

If no primary key constraint is specified, the first column in the table will be designated as the primary key.

KarelDB extends Calcite's SQL grammar by adding support for ALTER TABLE commands.

```
alterTableStatement:
    ALTER TABLE tableName columnAction [ , columnAction ]*
    
columnAction:
    ( ADD tableElement ) | ( DROP columnName )
```

KarelDB supports the following SQL types:

- boolean
- integer
- bigint
- real
- double
- varbinary
- varchar
- decimal
- date
- time
- timestamp

## Basic Configuration

KarelDB has a number of configuration properties that can be specified.  When using KarelDB as an embedded database, these properties should be prefixed with `schema.` before passing them to the JDBC driver.

- `listeners` - List of listener URLs that include the scheme, host, and port.  Defaults to `http://0.0.0.0:8765`.  
- `cluster.group.id` - The group ID to be used for leader election.  Defaults to `kareldb`.
- `leader.eligibility` - Whether this node can participate in leader election.  Defaults to true.
- `rocksdb.enable` - Whether to use RocksDB in KCache.  Defaults to true.  Otherwise an in-memory cache is used.
- `rocksdb.root.dir` - The root directory for RocksDB storage.  Defaults to `/tmp`.
- `kafkacache.bootstrap.servers` - A list of host and port pairs to use for establishing the initial connection to Kafka.
- `kafkacache.group.id` - The group ID to use for the internal consumers, which needs to be unique for each node.  Defaults to `kareldb-1`.
- `kafkacache.topic.replication.factor` - The replication factor for the internal topics created by KarelDB.  Defaults to 3.
- `kafkacache.init.timeout.ms` - The timeout for initialization of the Kafka cache, including creation of internal topics.  Defaults to 300 seconds.
- `kafkacache.timeout.ms` - The timeout for an operation on the Kafka cache.  Defaults to 60 seconds.

## Security

### HTTPS

To use HTTPS, first configure the `listeners` with an `https` prefix, then specify the following properties with the appropriate values.

```
ssl.keystore.location=/var/private/ssl/custom.keystore
ssl.keystore.password=changeme
ssl.key.password=changeme
```

When using the Avatica JDBC client, the `truststore` and `truststore_password` can be passed in the JDBC URL as specified [here](https://calcite.apache.org/avatica/docs/client_reference.html#truststore).

### HTTP Authentication

KarelDB supports both HTTP Basic Authentication and HTTP Digest Authentication, as shown below:

```
authentication.method=BASIC  # or DIGEST
authentication.roles=admin,developer,user
authentication.realm=KarelDb-Props  # as specified in JAAS file
```

In the above example, the JAAS file might look like

```
KarelDb-Props {
  org.eclipse.jetty.jaas.spi.PropertyFileLoginModule required
  file="/path/to/password-file"
  debug="false";
};
```

The `ProperyFileLoginModule` can be replaced with other implementations, such as `LdapLoginModule` or `JDBCLoginModule`.

When starting KarelDB, the path to the JAAS file must be set as a system property.

```bash
$ export KARELDB_OPTS=-Djava.security.auth.login.config=/path/to/the/jaas_config.file
$ bin/kareldb-start config/kareldb-secure.properties
```

When using the Avatica JDBC client, the `avatica_user` and `avatica_password` can be passed in the JDBC URL as specified [here](https://calcite.apache.org/avatica/docs/client_reference.html#avatica-user).

### Kafka Authentication

Authentication to a secure Kafka cluster is described [here](https://github.com/rayokota/kcache#security).
 
## Implementation Notes

KarelDB stores table data in topics of the form `{tableName}_{generation}`.  A different generation ID is used whenever a table is dropped and re-created.

KarelDB uses three topics to hold metadata:

- `_tables` - A topic that holds the schemas for tables.
- `_commits` - A topic that holds the list of committed transactions.
- `_timestamps` - A topic that stores the maximum timestamp that the transaction manager is allowed to return to clients.

## Database by Components

KarelDB is an example of a database built mostly by assembling pre-existing components.  In particular, KarelDB uses the following:

- [Apache Kafka](https://kafka.apache.org) - for persistence, using [KCache](https://github.com/rayokota/kcache) as an embedded key-value store
- [Apache Avro](https://avro.apache.org) - for serialization and schema evolution
- [Apache Calcite](https://calcite.apache.org) - for SQL parsing, optimization, and execution
- [Apache Omid](https://omid.incubator.apache.org) - for transaction management and MVCC support
- [Apache Avatica](https://calcite.apache.org/avatica/) - for JDBC functionality

See this [blog](https://yokota.blog/2019/09/23/building-a-relational-database-using-kafka) for more on the design of KarelDB.

## Future Enhancements 

Possible future enhancements include support for secondary indices.
