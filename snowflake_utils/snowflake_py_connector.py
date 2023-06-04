import snowflake.connector
import logging
from snowflake_utils.snow_logger import apply_log_config
from snowflake_utils.snow_config import SnowConfig
from utils import get_caller_info

logger = logging.getLogger(__name__)
apply_log_config(logger)


class SnowFlakeUtilsConnection:

    def __init__(self, snow_config: SnowConfig) -> None:
        self.user = snow_config.user
        self.password = snow_config.password
        self.account = snow_config.account
        self.warehouse = snow_config.warehouse
        self.database = snow_config.database
        self.schema = snow_config.schema
        self.role = snow_config.role
        self.network_timeout = snow_config.network_timeout
        self.login_timeout = snow_config.login_timeout
        self.timezone = snow_config.timezone
        self.autocommit = snow_config.autocommit
        self.client_prefetch_threads = snow_config.client_prefetch_threads
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            n, l = get_caller_info()
            logger.info(f"Connecting to snowflake from {n} at line {l}")
            self.connection = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
                role=self.role,
                network_timeout=self.network_timeout,
                login_timeout=self.login_timeout,
                timezone=self.timezone,
                autocommit=self.autocommit,
                client_prefetch_threads=self.client_prefetch_threads
            )
            logger.info("Connected to snowflake successfully")
        except Exception as e:
            logger.error("Error in connecting to snowflake")
            logger.error(e)
            raise e

    def cursor(self):
        try:
            n, l = get_caller_info()
            logger.info(f"Creating cursor from {n} at line {l}")
            self.cursor = self.connection.cursor()
            logger.info("Cursor created successfully")
        except Exception as e:
            logger.error("Error in creating cursor")
            logger.error(e)
            raise e

    def execute(self, sql):
        try:
            n, l = get_caller_info()
            logger.info(f"Executing from {n} at line {l}")
            logger.info(f"Executing sql: {sql}")
            self.cursor.execute(sql)
            logger.info("Sql executed successfully")
        except Exception as e:
            logger.error("Error in executing sql")
            logger.error(e)
            raise e

    def fetch(self):
        try:
            n, l = get_caller_info()
            logger.info(f"Fetching from {n} at line {l}")
            return self.cursor.fetchall()
        except Exception as e:
            logger.error("Error in fetching")
            logger.error(e)
            raise e
    
    def commit(self):
        try:
            n, l = get_caller_info()
            logger.info(f"Committing from {n} at line {l}")
            self.connection.commit()
            logger.info("Commit successful")
        except Exception as e:
            logger.error("Error in committing")
            logger.error(e)
            raise e
        
    def close(self):
        try:
            n, l = get_caller_info()
            logger.info(f"Closing connection from {n} at line {l}")
            self.connection.close()
            logger.info("Connection closed successfully")
        except Exception as e:
            logger.error("Error in closing connection")
            logger.error(e)
            raise e
    def is_closed(self):
        try:
            n, l = get_caller_info()
            logger.info(f"Checking if connection is closed from {n} at line {l}")
            return self.connection.is_closed()
        except Exception as e:
            logger.error("Error in checking if connection is closed")
            logger.error(e)
            raise e
    
    def fetchOne(self):
        try:
            n, l = get_caller_info()
            logger.info(f"Fetching one from {n} at line {l}")
            return self.cursor.fetchone()
        except Exception as e:
            logger.error("Error in fetching one")
            logger.error(e)
            raise e
        
    def fetchMany(self, size):
        try:
            n, l = get_caller_info()
            logger.info(f"Fetching many from {n} at line {l}")
            return self.cursor.fetchmany(size)
        except Exception as e:
            logger.error("Error in fetching many")
            logger.error(e)
            raise e
        
    def fetchAll(self):
        try:
            n, l = get_caller_info()
            logger.info(f"Fetching all from {n} at line {l}")
            return self.cursor.fetchall()
        except Exception as e:
            logger.error("Error in fetching all")
            logger.error(e)
            raise e
