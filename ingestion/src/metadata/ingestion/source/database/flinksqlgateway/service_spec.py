from metadata.utils.service_spec.default import DefaultDatabaseSpec
from metadata.ingestion.source.database.flinksqlgateway.metadata import FlinkSqlGatewaySource
# from metadata.ingestion.source.database.flinksqlgateway.lineage import FlinkSqlGatewayLineageSource

ServiceSpec = DefaultDatabaseSpec(
    metadata_source_class=FlinkSqlGatewaySource,
    # lineage_source_class=FlinkSqlGatewayLineageSource,
)