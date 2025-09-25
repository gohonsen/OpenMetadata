import requests
import time

from metadata.utils.logger import ingestion_logger
logger = ingestion_logger()

class FlinkSQLGatewayClient:
    def __init__(self, connect_url="http://127.0.0.1:8083/v1/sessions", catalog_name="default_catalog", database_name=None):
        self.connect_url = connect_url
        self.catalog_name = catalog_name
        self.database_name = database_name
        self.session_handle = None

    def create_session(self) -> bool:
        try:
            response = requests.post(self.connect_url)
            response.raise_for_status()
            self.session_handle = response.json()["sessionHandle"]
            logger.info(f"Flink Sql Gateway Session created: {self.session_handle}")
            use_catalog_handle = self.execute_statement(f"USE CATALOG {self.catalog_name}")
            if not use_catalog_handle:
                return False
            return True
        except Exception as e:
            logger.error(f"Failed to create flink sql gateway session: {e}")
            return False

    def execute_statement(self, statement) -> str:
        """execute sql"""
        if not self.session_handle:
            logger.error("No active session. Please create a session first.")
            return None

        url = f"{self.connect_url}/{self.session_handle}/statements"
        try:
            data = {"statement": statement}
            response = requests.post(url, json=data)
            response.raise_for_status()
            operation_handle = response.json()["operationHandle"]
            logger.info(f"Statement executed, operation handle: {operation_handle}")
            return operation_handle
        except Exception as e:
            logger.error(f"Failed to execute statement: {e}")
            return None

    def wait_for_result(self, operation_handle, max_retries=30, retry_interval=1):
        """Wait for the operation to complete and obtain the result"""
        result_url = f"{self.connect_url}/{self.session_handle}/operations/{operation_handle}/result/0"
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(result_url)
                response.raise_for_status()
                result_data = response.json()

                result_type = result_data.get("resultType")
                if result_type == "NOT_READY":
                    logger.info(f"Attempt {attempt}/{max_retries}: Result not ready, retrying...")
                    time.sleep(retry_interval)
                    continue
                elif result_type in ["PAYLOAD", "EOS"]:
                    return result_data
                else:
                    logger.error(f"Unexpected resultType: {result_type}")
                    return None
            except Exception as e:
                logger.error(f"Request failed on attempt {attempt}: {e}")
                return None

        logger.error(f"Timeout after {max_retries} attempts")
        return None

    def close_session(self):
        """close session"""
        if not self.session_handle:
            return
        url = f"{self.connect_url}/{self.session_handle}"
        try:
            response = requests.delete(url)
            response.raise_for_status()
            logger.info(f"Session {self.session_handle} closed")
        except Exception as e:
            logger.error(f"Warning: close session {self.session_handle} failed: {e}")


    def get_database(self, database_name=None) -> list:
        """get database list in catalog"""
        if database_name:
            return [database_name]

        show_database_handle = self.execute_statement("SHOW DATABASES")
        if not show_database_handle:
            return None

        result = self.wait_for_result(show_database_handle)
        if not result:
            return None
        if "results" in result and "data" in result["results"]:
            data_list = [database_entry["fields"][0] for database_entry in result["results"]["data"]]
            return data_list
        return None

    def get_tables(self, database_name) -> list:
        """get tables list in catalog and database"""
        # use_catalog_handle = self.execute_statement(f"USE CATALOG {catalog_name}")
        # if not use_catalog_handle:
        #     return None
        # time.sleep(1)
        use_db_handle = self.execute_statement(f"USE {database_name}")
        if not use_db_handle:
            return None
        time.sleep(1)

        show_tables_handle = self.execute_statement("SHOW TABLES")
        if not show_tables_handle:
            return None
        result = self.wait_for_result(show_tables_handle)
        if not result:
            return None
        if "results" in result and "data" in result["results"]:
            table_list = [table_entry["fields"][0] for table_entry in result["results"]["data"]]
            return table_list
        return None


    def get_table_columns(self, database_name, table_name) -> list:
        """get table columns"""
        use_db_handle = self.execute_statement(f"USE {database_name}")
        if not use_db_handle:
            return None
        time.sleep(1)

        describe_table_handle = self.execute_statement(f"DESCRIBE {table_name}")
        if not describe_table_handle:
            return None
        result = self.wait_for_result(describe_table_handle)
        if not result:
            return None

        columns = []
        if "results" in result and "columns" in result["results"]:
            for column_entry in result["results"]["data"]:
                column_name = column_entry["fields"][0]
                column_type = column_entry["fields"][1]
                column_nullable = column_entry["fields"][2]
                column_key = column_entry["fields"][3]
                column_comment = column_entry["fields"][6]
                columns.append(
                    {
                        "name": column_name,
                        "type": column_type,
                        "comment": column_comment,
                        "nullable": column_nullable,
                    }
                )
        return columns

    def get_table_comment(self, connection, table_name, schema_name, **kw):
        """
        Returns comment of table.
        """
        return {"text": None}
