import os 
from dotenv import load_dotenv

load_dotenv()

# SharePoint Credentials
SHAREPOINT_SITE_URL = os.getenv('SHAREPOINT_SITE_URL')
SHAREPOINT_USERNAME = os.getenv('SHAREPOINT_USERNAME')
SHAREPOINT_PASSWORD = os.getenv('SHAREPOINT_PASSWORD')
SHAREPOINT_BASE_FOLDER = os.getenv('SHAREPOINT_BASE_FOLDER', 'Documentos Compartidos')
SHAREPOINT_FOLDER_PATH = os.getenv('SHAREPOINT_FOLDER_PATH', '')

# Database connection
DATABASE_HOST = os.getenv('DATABASE_HOST', '127.0.0.1')
DATABASE_PORT = int(os.getenv('DATABASE_PORT', 3306))  
DATABASE_NAME = os.getenv('DATABASE_NAME', 'erc_db')
DATABASE_USER = os.getenv('DATABASE_USER', 'erc_user')
DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD', 'erc_password')
DATABASE_URL = f"mysql+mysqlconnector://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"

# Configure logging
LOGGING_LEVEL = os.getenv('LOGGING_LEVEL', 'INFO')
LOGGING_FILE = os.getenv('LOGGING_FILE', "etl_process.log")

# Folder paths
# RAW_DATA_PATH = os.getenv('RAW_DATA_PATH', 'data/raw')
# PROCESSED_DATA_PATH = os.getenv('PROCESSED_DATA_PATH', 'data/processed')

# Ensure paths exist
# os.makedirs(RAW_DATA_PATH, exist_ok=True)
# os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
os.makedirs(os.path.dirname(LOGGING_FILE), exist_ok=True)