import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

try:
    from prefect import get_run_logger
    PREFECT_AVAILABLE = True
except ImportError:
    PREFECT_AVAILABLE = False

class ETLLogger:
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password
        self.engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/etl_logs")
        self._ensure_logging_database()
        
        # Set up standard Python logger as fallback
        self.python_logger = logging.getLogger('etl_pipeline')
        self.python_logger.setLevel(logging.INFO)
        if not self.python_logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.python_logger.addHandler(handler)

    def _get_logger(self):
        """Get appropriate logger based on context"""
        if PREFECT_AVAILABLE:
            try:
                return get_run_logger()
            except Exception:
                # Fall back to Python logger if Prefect context not available
                return self.python_logger
        else:
            return self.python_logger

    def _ensure_logging_database(self):
        """Create logging database and tables if they don't exist"""
        try:
            base_engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}/")
            with base_engine.connect() as conn:
                conn.execute(text("CREATE DATABASE IF NOT EXISTS etl_logs"))
            
            # Create tables for logging
            with self.engine.connect() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS pipeline_runs (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        run_id VARCHAR(255),
                        start_time DATETIME,
                        end_time DATETIME,
                        success BOOLEAN,
                        error_message TEXT,
                        gcp_job_url VARCHAR(500),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS api_calls (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        platform VARCHAR(100),
                        client VARCHAR(255),
                        endpoint VARCHAR(255),
                        status_code INT,
                        success BOOLEAN,
                        duration_seconds FLOAT,
                        payload_size BIGINT,
                        error_message TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS data_operations (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        run_id VARCHAR(255),
                        client VARCHAR(255),
                        table_name VARCHAR(255),
                        rows_affected INT,
                        operation_type VARCHAR(50),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                conn.commit()
                
        except Exception as e:
            self.python_logger.error(f"Failed to set up logging database: {e}")

    def log_pipeline_run(self, run_id, start_time, end_time, success, error_message=None, gcp_job_url=None):
        """Log pipeline execution details"""
        logger = self._get_logger()
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO pipeline_runs (run_id, start_time, end_time, success, error_message, gcp_job_url)
                    VALUES (:run_id, :start_time, :end_time, :success, :error_message, :gcp_job_url)
                """), {
                    'run_id': run_id,
                    'start_time': start_time,
                    'end_time': end_time,
                    'success': success,
                    'error_message': error_message,
                    'gcp_job_url': gcp_job_url
                })
                conn.commit()
            
            status = "SUCCESS" if success else "FAILED"
            logger.info(f"Pipeline run {run_id} completed with status: {status}")
            if error_message:
                logger.error(f"Pipeline error: {error_message}")
                
        except Exception as e:
            logger.error(f"Failed to log pipeline run: {e}")

    def log_api_call(self, platform, client, endpoint, status_code, success, duration, payload_size, error_message=None):
        """Log API call details"""
        logger = self._get_logger()
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO api_calls (platform, client, endpoint, status_code, success, duration_seconds, payload_size, error_message)
                    VALUES (:platform, :client, :endpoint, :status_code, :success, :duration, :payload_size, :error_message)
                """), {
                    'platform': platform,
                    'client': client,
                    'endpoint': endpoint,
                    'status_code': status_code,
                    'success': success,
                    'duration': duration,
                    'payload_size': payload_size,
                    'error_message': error_message
                })
                conn.commit()
            
            status_msg = f"{platform} API call for {client}: {status_code} ({duration}s)"
            if success:
                logger.info(status_msg)
            else:
                logger.error(f"{status_msg} - Error: {error_message}")
                
        except Exception as e:
            logger.error(f"Failed to log API call: {e}")

    def log_rows_appended(self, run_id, client, table_name, row_count):
        """Log data operation details"""
        logger = self._get_logger()
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO data_operations (run_id, client, table_name, rows_affected, operation_type)
                    VALUES (:run_id, :client, :table_name, :rows_affected, :operation_type)
                """), {
                    'run_id': run_id,
                    'client': client,
                    'table_name': table_name,
                    'rows_affected': row_count,
                    'operation_type': 'INSERT'
                })
                conn.commit()
            
            logger.info(f"Inserted {row_count} rows into {table_name} for client {client}")
            
        except Exception as e:
            logger.error(f"Failed to log data operation: {e}")
