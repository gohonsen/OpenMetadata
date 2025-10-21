#  Copyright 2025 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
Flink SQL Gateway source implementation.
Useful for testing!
"""
import traceback
from typing import Iterable, Optional, Tuple, List, cast
from sqlalchemy.engine.reflection import Inspector
from metadata.ingestion.api.models import Either
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.type.basic import (
    EntityName,
    FullyQualifiedEntityName,
    Markdown,
)
from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.ingestion.source.database.flinksqlgateway.connection import get_connection, test_connection
from metadata.generated.schema.entity.services.connections.database.flinkSqlGatewayConnection import (
    FlinkSqlGatewayConnection,
)
from metadata.ingestion.source.database.common_db_source import (
    CommonDbSourceService,
    TableNameAndType
)
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.entity.data.table import (
    Column,
    ConstraintType,
    Table,
    TableConstraint,
    TablePartition,
    TableType,
)
from metadata.ingestion.source.database.database_service import DatabaseServiceSource
from metadata.utils import fqn
from metadata.utils.filters import filter_by_database, filter_by_schema, filter_by_table
from metadata.utils.logger import ingestion_logger
from metadata.utils.execution_time_tracker import (
    calculate_execution_time,
    calculate_execution_time_generator,
)
logger = ingestion_logger()


class FlinkSqlGatewaySource(CommonDbSourceService):
    # @retry_with_docker_host()
    # def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
    #     # self.test_connection = lambda: None
    #     self.ssl_manager = None
    #     self.client = None
    #     self.session = None
    #
    #     self.config = config
    #     self.source_config: DatabaseServiceMetadataPipeline = (
    #         self.config.sourceConfig.config
    #     )
    #     # It will be one of the Unions. We don't know the specific type here.
    #     self.service_connection = self.config.serviceConnection.root.config
    #     self.engine = get_connection(self.service_connection)
    #     self.connection_obj = self.engine
    #     self.metadata = metadata
    #
    #     self._connection_map = {}  # Lazy init as well
    #     # self._inspector_map = {}
    #     # self.table_constraints = None
    #     # self.database_source_state = set()
    #     # self.context.get_global().table_constrains = []
    #     # self.context.get_global().foreign_tables = []
    #     # self.context.set_threads(self.source_config.threads)
    #
    #     # Flag the connection for the test connection
    #     self.test_connection = self._test_connection
    #     self.test_connection()
    #
    #     # Flag the connection for the test connection
    #     # self.connection: FlinkSqlGatewayConnection = config.connection
    #     self.connect_url = f"{self.service_connection.hostPort}v1/sessions"
    #     self.catalog_name = self.service_connection.catalog
    #     self.database_name = self.service_connection.database
    #     self.session_handle = None
    #     logger.info(f"Flink Sql Gateway init: {self.connect_url}/{self.catalog_name}")

    def _test_connection(self) -> None:
        logger.info("call test connection")
        test_connection(self.metadata, self.engine, self.service_connection)

    # @property
    # def connection(self) -> Connection:
    #     """
    #     Return the SQLAlchemy connection
    #     """
    #     thread_id = self.context.get_current_thread_id()
    #     if not self._connection_map.get(thread_id):
    #         self._connection_map[thread_id] = self.engine.connect()
    #     return self._connection_map[thread_id]

    @classmethod
    def create(
            cls, config_dict, metadata: OpenMetadata, pipeline_name: Optional[str] = None
    ):
        config: WorkflowSource = WorkflowSource.model_validate(config_dict)
        # connection = config.serviceConnection.root.config
        connection = cast(FlinkSqlGatewayConnection, config.serviceConnection.root.config)
        if not isinstance(connection, FlinkSqlGatewayConnection):
            raise InvalidSourceException(
                f"Expected FlinkSqlGatewayConnection, but got {connection}"
            )
        return cls(config, metadata)

    def get_database_names(self) -> Iterable[str]:
        """
        Default case with a single database.

        It might come informed - or not - from the source.

        Sources with multiple databases should overwrite this and
        apply the necessary filters.
        """
        custom_database_name = self.service_connection.__dict__.get("databaseName")
        database_name = self.service_connection.__dict__.get(
            "database", custom_database_name or "default"
        )
        logger.info(f"Flink Sql Gateway get database name:{database_name}")

        yield database_name

    @calculate_execution_time_generator()
    def yield_database(
            self, database_name: str
    ) -> Iterable[Either[CreateDatabaseRequest]]:
        """
        From topology.
        Prepare a database request and pass it to the sink
        """

        description = None
        source_url = None
        database_request = CreateDatabaseRequest(
            name=EntityName(database_name),
            service=FullyQualifiedEntityName(self.context.get().database_service),
            description=description,
            sourceUrl=source_url,
            tags=self.get_database_tag_labels(database_name=database_name),
        )

        yield Either(right=database_request)
        self.register_record_database_request(database_request=database_request)

    @staticmethod
    @calculate_execution_time()
    def get_table_description(
            schema_name: str, table_name: str, inspector: Inspector
    ) -> str:
        description = None
        try:
            table_info: dict = inspector.get_table_comment(table_name, schema_name)
            logger.info(f"Table info: {type(table_info)}  {table_info}")
        # Catch any exception without breaking the ingestion
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug(traceback.format_exc())
            logger.warning(
                f"Table description error for table [{schema_name}.{table_name}]: {exc}"
            )
        else:
            description = table_info.get("text")
        return description

    # def query_table_names_and_types(
    #         self, schema_name: str
    # ) -> Iterable[TableNameAndType]:
    #     return [
    #         TableNameAndType(name=table_name)
    #         for table_name in self.inspector.get_table_names(schema_name) or []
    #     ]

    def get_tables_name_and_type(self) -> Optional[Iterable[Tuple[str, str]]]:
        """
        Handle table and views.

        Fetches them up using the context information and
        the inspector set when preparing the db.

        :return: tables or views, depending on config
        """
        schema_name = self.context.get().database_schema
        try:
            if self.source_config.includeTables:
                for table_and_type in self.query_table_names_and_types(schema_name):
                    table_name = self.standardize_table_name(
                        schema_name, table_and_type.name
                    )
                    table_fqn = fqn.build(
                        self.metadata,
                        entity_type=Table,
                        service_name=self.context.get().database_service,
                        database_name=self.context.get().database,
                        schema_name=self.context.get().database_schema,
                        table_name=table_name,
                        skip_es_search=True,
                    )
                    if filter_by_table(
                            self.source_config.tableFilterPattern,
                            (
                                    table_fqn
                                    if self.source_config.useFqnForFiltering
                                    else table_name
                            ),
                    ):
                        self.status.filter(
                            table_fqn,
                            "Table Filtered Out",
                        )
                        continue
                    yield table_name, table_and_type.type_
        except Exception as err:
            logger.warning(
                f"Fetching tables names failed for schema {schema_name} due to - {err}"
            )
            logger.debug(traceback.format_exc())

    @calculate_execution_time_generator()
    def yield_table(
            self, table_name_and_type: Tuple[str, TableType]
    ) -> Iterable[Either[CreateTableRequest]]:
        """
        From topology.
        Prepare a table request and pass it to the sink
        """
        table_name, table_type = table_name_and_type
        schema_name = self.context.get().database_schema
        try:
            (
                columns,
                table_constraints,
                foreign_columns,
            ) = self.get_columns_and_constraints(
                schema_name=schema_name,
                table_type=table_type,
                table_name=table_name,
                db_name=self.context.get().database,
                inspector=self.inspector,
            )

            schema_definition = self.get_schema_definition(
                table_type=table_type,
                table_name=table_name,
                schema_name=schema_name,
                inspector=self.inspector,
            )

            table_constraints = self.update_table_constraints(
                schema_name=schema_name,
                table_name=table_name,
                db_name=self.context.get().database,
                table_constraints=table_constraints,
                foreign_columns=foreign_columns,
                columns=columns,
            )

            description = (
                Markdown(db_description)
                if (
                    db_description := self.get_table_description(
                        schema_name=schema_name,
                        table_name=table_name,
                        inspector=self.inspector,
                    )
                )
                else None
            )

            table_request = CreateTableRequest(
                name=EntityName(table_name),
                tableType=table_type,
                description=description,
                columns=columns,
                tableConstraints=table_constraints,
                schemaDefinition=schema_definition,
                databaseSchema=FullyQualifiedEntityName(
                    fqn.build(
                        metadata=self.metadata,
                        entity_type=DatabaseSchema,
                        service_name=self.context.get().database_service,
                        database_name=self.context.get().database,
                        schema_name=schema_name,
                    )
                ),
                tags=self.get_tag_labels(
                    table_name=table_name
                ),  # Pick tags from context info, if any
                # sourceUrl=self.get_source_url(
                #     table_name=table_name,
                #     schema_name=schema_name,
                #     database_name=self.context.get().database,
                #     table_type=table_type,
                # ),
                sourceUrl = None,
                owners=self.get_owner_ref(table_name=table_name),
                locationPath=self.get_location_path(
                    table_name=table_name, schema_name=schema_name
                ),
            )

            yield Either(right=table_request)
            # Register the request that we'll handle during the deletion checks
            self.register_record(table_request=table_request)
        except Exception as exc:
            error = (
                f"Unexpected exception to yield table "
                f"(database=[{self.context.get().database}], schema=[{schema_name}], table=[{table_name}]): {exc}"
            )
            yield Either(
                left=StackTraceError(
                    name=table_name, error=error, stackTrace=traceback.format_exc()
                )
            )