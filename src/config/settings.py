"""
Configuration settings for EDINET scraper
"""
import os

# Try to load a local .env automatically if python-dotenv is available
try:
    from dotenv import find_dotenv, load_dotenv
    dotenv_path = os.environ.get("DOTENV_PATH") or find_dotenv()
    if dotenv_path:
        load_dotenv(dotenv_path)
except Exception:
    # python-dotenv is optional; proceed using existing environment variables
    pass

#Constants
PLATFORM = os.getenv("PLATFORM", "SGX")

# Base URLs
SGX_BASE_URL = os.getenv("SGX_BASE_URL", "https://www.sgx.com/")
SGX_COMPANY_API_URL = os.getenv("SGX_COMPANY_API_URL", "https://api.sgx.com/announcements/v1.1/company")
SGX_RESULTS_COUNT_API_URL = os.getenv("SGX_RESULTS_COUNT_API_URL", "https://api.sgx.com/announcements/v1.1/company/count")
CMS_URL = os.getenv("CMS_URL", "https://api2.sgx.com/content-api/?queryId=17d94f69435775a0d673d1b5328b0403ce4ad025:we_chat_qr_validator")
ATTACHMENTS_BASE_URL = os.getenv("ATTACHMENTS_BASE_URL", "https://links.sgx.com")


# MongoDB / queue settings
MONGODB_URI = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI") or os.getenv("MONGOURL")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE") or os.getenv("MONGO_DB") or os.getenv("MONGO_DATABASE")
COMPANIES_QUEUE_COLLECTION = os.getenv("COMPANIES_QUEUE_COLLECTION") or os.getenv("MONGODB_COLLECTION") or os.getenv("MONGODB_COLLECTION") or "companies"
PUBLIC_DOCUMENTS_COLLECTION = os.getenv("PUBLIC_DOCUMENTS_COLLECTION") or os.getenv("PUBLIC_DOCUMENTS") or os.getenv("FILES_COLLECTION") or "files"
COMPANIES_PROD_COLLECTION = os.getenv("COMPANIES_PROD_COLLECTION") or "sgx-documents-prod"
COMPANIES_UAT_COLLECTION = os.getenv("COMPANIES_UAT_COLLECTION") or "sgx-documents-uat"
UNLISTED_COMPANIES_COLLECTION = os.getenv("UNLISTED_COMPANIES_COLLECTION") or "sgx-documents-unlisted"


MAX_COMPANIES = int(os.getenv("MAX_COMPANIES", "0"))  # 0 means no limit
MAX_FILES_PER_COMPANY = int(os.getenv("MAX_FILES_PER_COMPANY", "20"))  # Max files to download per company
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))  # Max concurrent workers for downloading files
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
BACKOFF_FACTOR = int(os.getenv("BACKOFF_FACTOR", "2"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))  # Timeout for HTTP requests in seconds

UAT_PROD_MODE = os.getenv("UAT_PROD_MODE")  # UAT or PROD

# S3 / storage
S3_ENABLED = str(os.getenv("S3_ENABLED", "false")).lower() in ("1", "true", "yes")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_BASE_PATH = os.getenv("S3_BASE_PATH")
S3_REGION = os.getenv("S3_REGION")
STORAGE_MODE = os.getenv("STORAGE_MODE", "local")
CLEANUP_LOCAL_FILES = str(os.getenv("CLEANUP_LOCAL_FILES", "false")).lower() in ("1", "true", "yes")

# Data storage
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")
if not RAW_DATA_DIR:
    if S3_ENABLED:
        RAW_DATA_DIR = f"s3://{S3_BUCKET_NAME}/{S3_BASE_PATH or 'data'}"
    else:
        RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "raw_data_storage")



