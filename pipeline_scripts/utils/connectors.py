from configparser import ConfigParser
import pymysql
from sqlalchemy import create_engine
import logging
import os
from datetime import datetime

# Create a folder for today's date
today_date = datetime.now().strftime('%Y-%m-%d')
log_folder = os.path.join('pipeline_scripts', 'logs', today_date)
os.makedirs(log_folder, exist_ok=True)

# Configure module-specific logging to write to a file in the dated folder
log_config_file_path = os.path.join(log_folder, 'connectors.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler = logging.FileHandler(log_config_file_path)
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

def load_config(config_file_path):
    """
    Load the configuration file.
    
    Parameters:
    config_file_path (str): The path to the configuration file.
    
    Returns:
    ConfigParser: The loaded configuration parser object.
    """
    parser = ConfigParser()
    if not os.path.isfile(config_file_path):
        logging.error(f"Configuration file not found: {config_file_path}")
        raise FileNotFoundError(f"Configuration file not found: {config_file_path}")
    
    try:
        parser.read(config_file_path)
        logging.info(f"Configuration loaded from {config_file_path}")
        return parser
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        raise Exception(f"Error loading configuration: {e}")

def mysql_connect_to_db(env):
    """
    Connect to the MySQL database using the given environment configuration.
    
    Parameters:
    env (str): The environment to use (e.g., 'production', 'development').
    parser (ConfigParser): The loaded configuration parser object.
    
    Returns:
    engine : The MySQL database connection object.
    """
    config_file_path = 'pipeline_scripts/vault/credentials.config'
    parser = load_config(config_file_path)

    try:
        host = parser.get(env, 'host')
        user = parser.get(env, 'user')
        password = parser.get(env, 'password')
        database = parser.get(env, 'database')

        # Create SQLAlchemy engine
        connection_string = f"mysql+pymysql://{user}:{password}@{host}/{database}"
        engine = create_engine(connection_string)

        logging.info(f"Connected to MySQL database: {database} at {host}")
        return engine

    except pymysql.MySQLError as e:
        logging.error(f"MySQL error: {e}")
        raise Exception(f"MySQL error: {e}")

    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        raise Exception(f"Error connecting to database: {e}")