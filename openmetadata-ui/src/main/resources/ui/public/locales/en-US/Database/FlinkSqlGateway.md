# FlinkSqlGateway
In this section, we provide guides and references to use the FlinkSqlGateway connector.

## Requirements

OpenMetadata is integrated with flink up to version <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/gettingstarted/" target="_blank">1.19.0</a> and will continue to work for future flink versions.

The ingestion framework uses flink APIs to connect to the flink sql gateway instance and perform the API calls

## Connection Details
$$section
### Host and Port $(id="hostPort")
This parameter specifies the host and port of the Flink Sql Gateway Service. This should be specified as a string in the format `host:port`. For example, you might set the hostPort parameter to `127.0.0.1:8083`.
$$

$$section
### Flink Catalog Name $(id="catalogName")
This parameter specifies a catalog name of the Flink.
Flink catalogs provide metadata, such as databases, tables, partitions, views, and functions and information needed to access data stored in a database or other external systems.
This should be specified as a string in the format `hive_catalog`.
$$


$$section
### Flink Database Name $(id="databaseName")
In OpenMetadata, the Database Service hierarchy works as follows:
```
Database Service > Database > Schema > Table
```
In the case of Flink Sql Gateway, we won't have a Schema as such. If you'd like to see your data in a database, you need specify the name in this field.
$$
