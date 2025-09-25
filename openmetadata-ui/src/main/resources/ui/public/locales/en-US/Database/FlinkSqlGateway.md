# FlinkSqlGateway
In this section, we provide guides and references to use the FlinkSqlGateway connector.

## Requirements

OpenMetadata is integrated with flink up to version <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/gettingstarted/" target="_blank">1.19.0</a> and will continue to work for future flink versions.

The ingestion framework uses flink APIs to connect to the flink sql gateway instance and perform the API calls

## Connection Details
$$section
### Host and Port $(id="hostPort")
Flink Sql Gateway Service URI. This should be specified as a URI string in the format `scheme://hostname:port`. E.g., `http://localhost:8083`, `http://host.docker.internal:8083`.
$$

$$section
### Flink Catalog Name $(id="catalog")

$$


$$section
### database name $(id="database")
`Optional`.
$$
