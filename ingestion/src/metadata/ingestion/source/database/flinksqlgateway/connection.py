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
Source connection handler
"""
from typing import Optional
from functools import partial
from sqlalchemy.engine import Engine
from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
)
from metadata.generated.schema.entity.services.connections.database.flinkSqlGatewayConnection import (
    FlinkSqlGatewayConnection as FlinkSqlGatewayConnectionConfig,
)
from metadata.generated.schema.entity.services.connections.testConnectionResult import (
    TestConnectionResult,
    TestConnectionStepResult,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.connections.builders import (
    create_generic_db_connection,
    get_connection_args_common,
)
from metadata.ingestion.connections.test_connections import (
    test_connection_db_common,
    execute_inspector_func,
    test_connection_engine_step,
    test_connection_steps,
    test_query,
)

from metadata.utils.constants import THREE_MIN
from metadata.utils.logger import ingestion_logger
logger = ingestion_logger()


def create_engine(connection: FlinkSqlGatewayConnectionConfig):
    return create_generic_db_connection(
        connection=connection,
        get_connection_url_fn=get_connection_url,
        get_connection_args_fn=get_connection_args_common,
    )
def get_connection_url(connection: FlinkSqlGatewayConnectionConfig) -> str:
    url = f"{connection.scheme.value}://{connection.hostPort}"
    if connection.catalogName:
        url += f"?catalog={connection.catalogName}"
    if connection.databaseName:
        url += f"&database={connection.databaseName}"
    # if connection.timeout:
    #     url += f"&timeout={connection.timeout}"
    # print(f"-----------flink connect url: {url}")
    return url

def get_connection(connection: FlinkSqlGatewayConnectionConfig) -> Engine:
    """
    Create engine object
    """
    logger.info(f"Flink Sql Gateway get connection: {FlinkSqlGatewayConnectionConfig}")
    return create_engine(connection)


def test_connection(
    metadata: OpenMetadata,
    engine: Engine,
    service_connection: FlinkSqlGatewayConnectionConfig,
    automation_workflow: Optional[AutomationWorkflow] = None,
    timeout_seconds: Optional[int] = THREE_MIN
) -> TestConnectionResult:
    """
    Test connection. This can be executed either as part
    of a metadata workflow or during an Automation Workflow
    """
    def test_get_catalogs(connection) -> TestConnectionStepResult:
        logger.info(f"Flink Sql Gateway test catalogs")
        # sql = 'SHOW CURRENT CATALOG'
        sql = 'SHOW CATALOGS'
        with connection.connect() as conn:
            result = conn.exec_driver_sql(sql)
            logger.info(f"Flink Sql Gateway catalog list{result.fetchall()}")
        return result.fetchall()

    def test_get_databases(connection) -> TestConnectionStepResult:
        logger.info(f"Flink Sql Gateway test databases")
        # sql = 'SHOW CURRENT DATABASE'
        sql = 'SHOW DATABASES'
        with connection.connect() as conn:
            result = conn.exec_driver_sql(sql)
            logger.info(f"Flink Sql Gateway database list{result.fetchall()}")
        return result.fetchall()

    def test_connection_inner(engine):
        test_fn = {
            "CheckAccess": partial(test_connection_engine_step, engine),
            "CheckCatalog": partial(test_get_catalogs, engine),
            "CheckDatabase": partial(test_get_databases, engine),
        }

        return test_connection_steps(
            metadata=metadata,
            service_type=service_connection.type.value,
            test_fn=test_fn,
            automation_workflow=automation_workflow,
            timeout_seconds=timeout_seconds,
        )

    return test_connection_inner(engine)
