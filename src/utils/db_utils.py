from pathlib import Path
import sys
ROOT_PATH = Path(__file__).resolve().parent.parent.parent
SRC_PATH = ROOT_PATH / "src"
sys.path.append(str(ROOT_PATH))
sys.path.append(str(SRC_PATH))

from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from config.settings import MONGODB_URI, MONGODB_DATABASE, COMPANIES_QUEUE_COLLECTION, PUBLIC_DOCUMENTS_COLLECTION, COMPANIES_UAT_COLLECTION, COMPANIES_PROD_COLLECTION



print(f"Using MongoDB service at { MONGODB_URI }")
print(f"Using database: { MONGODB_DATABASE  }")
print(f"Using queue collection: {COMPANIES_QUEUE_COLLECTION }")
print(f"Using files collection: { PUBLIC_DOCUMENTS_COLLECTION }")
print(f"Using company collection [UAT]: { COMPANIES_UAT_COLLECTION }")
print(f"Using company collection [PROD]: { COMPANIES_PROD_COLLECTION }")

def create_indexes(db):
    """Create indexes for MongoDB collections."""
    from pymongo.errors import PyMongoError
    try:
        db[COMPANIES_QUEUE_COLLECTION].create_index(
            [("company_id", 1)], unique=True, background=True
        )
        db[COMPANIES_QUEUE_COLLECTION].create_index(
            [("company_name", 1)], unique=True, background=True
        )

        db[PUBLIC_DOCUMENTS_COLLECTION].create_index(
            [("file_name", 1)], unique=True, background=True
        )
        db[PUBLIC_DOCUMENTS_COLLECTION].create_index(
            [("document_id", 1)], unique=True, background=True
        )

        db[COMPANIES_PROD_COLLECTION].create_index(
            [("company_id", 1)], unique=True, background=True
        )
        db[COMPANIES_PROD_COLLECTION].create_index(
            [("company_name", 1)], unique=True, background=True
        )

        db[COMPANIES_UAT_COLLECTION].create_index(
            [("company_id", 1)], unique=True, background=True
        )
        db[COMPANIES_UAT_COLLECTION].create_index(
            [("company_name", 1)], unique=True, background=True
        )

        print("Indexes created successfully.")
    except PyMongoError as e:
        print(f"Index creation error: {e}")

def connect_mongo(uri=None, db_name=None):
    """Connect to MongoDB and return the database object."""
    client = MongoClient(MONGODB_URI if uri is None else uri)
    return client[MONGODB_DATABASE if db_name is None else db_name]

# Create indexes and the connection when the file is imported
db = connect_mongo()
create_indexes(db)

def store_metadata_batch(metadata_list: list) -> None:
    """Store a batch of metadata documents in the database."""
    global db  # Use the global db object

    if not metadata_list:
        print("No metadata to store.")
        return

    collection = db[PUBLIC_DOCUMENTS_COLLECTION]
    try:
        result = collection.insert_many(metadata_list, ordered=False)
        print(f"Saved {len(result.inserted_ids)} documents in {PUBLIC_DOCUMENTS_COLLECTION}.")
    except BulkWriteError as bwe:
        duplicate_count = sum(1 for error in bwe.details.get("writeErrors", []) if (error.get("code") == 11000 and error.get("keyPattern") == {"file_name": 1}))
        added_count = len(metadata_list) - duplicate_count
        if added_count > 0:
            print(f"Saved {added_count} new documents in {PUBLIC_DOCUMENTS_COLLECTION}.")
        if duplicate_count > 0:
            print(f"{duplicate_count} documents already exist in the collection.")
            duplicate_files = []
            for error in bwe.details.get("writeErrors", []):
                # Manage file_name duplicates by appending a counter
                if error.get("code") == 11000 and error.get("keyPattern") == {"document_id": 1}:
                    # Handle duplicate based on document_id
                    duplicate_file = metadata_list[error["index"]]
                    doc_id = duplicate_file.get("document_id", "unknown")
                    print(f"Duplicate document_id found: {doc_id}. Skipping insertion.")
                elif error.get("code") == 11000 and error.get("keyPattern") == {"file_name": 1}:
                    duplicate_file = metadata_list[error["index"]]  # file that caused the duplicate error
                    base_name = duplicate_file.get("file_name", "unknown")  # get its file_name
                    counter = 1
                    new_file_name = f"{base_name} - [{counter}]"  # start with counter 1
                    while collection.find_one({"file_name": new_file_name}):  # verify if new name exists
                        counter += 1
                        new_file_name = f"{base_name} - [{counter}]"  # update new name and try again

                    duplicate_file["file_name"] = new_file_name

                    try:
                        collection.insert_one(duplicate_file)
                        print(f"Inserted duplicate with new name: {new_file_name}")
                    except Exception as e:
                        #print(f"Failed to insert duplicate file:{duplicate_file.get('document_id')}")
                        pass
                    duplicate_files.append(new_file_name)
    except Exception as e:
        print(f"Batch insert error: {e}")
    

__all__ = ["db", "connect_mongo", "store_metadata_batch"]

