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
            [("company_id", 1), ("name", 1)], unique=True, background=True
        )
        #db[COMPANIES_QUEUE_COLLECTION].create_index(
        #    [("name", 1)], unique=True, background=True
        #)

        db[PUBLIC_DOCUMENTS_COLLECTION].create_index(
            [("document_id", 1)], unique=True, background=True
        )
        
        db[PUBLIC_DOCUMENTS_COLLECTION].create_index(
            [("file_name", 1)], unique=True, background=True
        )
        

        db[COMPANIES_PROD_COLLECTION].create_index(
            [("company_id", 1)], unique=True, background=True
        )
        db[COMPANIES_PROD_COLLECTION].create_index(
            [("name", 1)], unique=True, background=True
        )

        db[COMPANIES_UAT_COLLECTION].create_index(
            [("company_id", 1)], unique=True, background=True
        )
        db[COMPANIES_UAT_COLLECTION].create_index(
            [("name", 1)], unique=True, background=True
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
        duplicate_count = sum(1 for error in bwe.details.get("writeErrors", []) if (error.get("code") == 11000 and error.get("keyPattern") == {"document_id": 1}))
        added_count = len(metadata_list) - duplicate_count
        if added_count > 0:
            print(f"Saved {added_count} new documents in {PUBLIC_DOCUMENTS_COLLECTION}.")
        if duplicate_count > 0:
            print(f"{duplicate_count} documents already exist in the collection.")
            duplicate_files = []
            for error in bwe.details.get("writeErrors", []):
                # Manage file_name duplicates by appending a counter
                #print(f"Duplicate error details: {error}")
                
                if error.get("code") == 11000 and error.get("keyPattern") == {"document_id": 1}:
                    # Handle duplicate based on document_id
                    #print("Handling duplicate document_id ...")
                    duplicate_file = metadata_list[error["index"]]
                    doc_id = duplicate_file.get("document_id", "unknown")
                    existing_doc = collection.find_one({"document_id": doc_id})
                    
                    if existing_doc:
                        new_supporting_file_paths = duplicate_file.get("supporting_file_paths", [])
                        existing_supporting_file_paths = existing_doc.get("supporting_file_paths", [])

                        # DEBUG PRINT
                        #print(f"Comparing supporting_file_paths lengths for document_id {doc_id}:")
                        #print(f" - New supporting_file_paths length: {len(new_supporting_file_paths)}")
                        #print(f" - Existing supporting_file_paths length: {len(existing_supporting_file_paths)}")

                        if len(new_supporting_file_paths) > len(existing_supporting_file_paths):
                            try:
                                # Exclude the '_id' field from the update to avoid modifying the immutable field
                                duplicate_file_without_id = {k: v for k, v in duplicate_file.items() if k != "_id"}
                                collection.update_one(
                                    {"document_id": doc_id},
                                    {"$set": duplicate_file_without_id}
                                )
                                print(f"Updated document_id {doc_id} with updated supporting_file_paths.")
                            except Exception as e:
                                print(f"Failed to update document_id {doc_id}: {e}")
                        else:
                            print(f"Document_id {doc_id} already exists with a newer version. Skipping update.")
                    else:
                        print(f"Duplicate document_id found: {doc_id}. Skipping insertion.")
                elif error.get("code") == 11000 and error.get("keyPattern") == {"file_name": 1}:
                    # Handle duplicate file_name only if document_id duplication did not occur
                    if not any(err.get("code") == 11000 and err.get("keyPattern") == {"document_id": 1} for err in bwe.details.get("writeErrors", [])):
                        #print("Handling duplicate file_name ...")
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

def store_company_queue(companylist):
    # Implement the logic to store the company queue in the database
    if not companylist:
        print("No companies to store.")
        return

    print(f"Passed {len(companylist)} entries to the inserting function.")
    db = connect_mongo()
    if db is None:
        raise ValueError("Database connection error.")

    collection = db[COMPANIES_QUEUE_COLLECTION]
    from datetime import datetime, timezone
    current_timestamp = datetime.now(timezone.utc)  # Get the current UTC timestamp

    for company in companylist:
        company["processed"] = False
        company["status"] = "pending"
        company["updated_at"] = current_timestamp

    try:
        result = collection.insert_many(companylist, ordered=False)
        print(f"Inserted {len(result.inserted_ids)} companies into the queue.")
    except BulkWriteError as bwe:
        duplicate_count = sum(1 for error in bwe.details.get("writeErrors", []) if error.get("code") == 11000)
        added_count = len(companylist) - duplicate_count
        if added_count > 0:
            print(f"Inserted {added_count} new companies into the queue.")
        if duplicate_count > 0:
            print(f"{duplicate_count} companies already exist in the queue. Skipping duplicates.")
    except Exception as e:
        print(f"Error inserting companies: {e}")

def get_pending_companies():
    # Implement the logic to retrieve pending companies from the database
    db = connect_mongo()
    if db is None:
        raise ValueError("Database connection error.")

    collection = db[COMPANIES_QUEUE_COLLECTION]
    try:
        pending_companies = list(collection.find({"status": "pending"}))
        print(f"Retrieved {len(pending_companies)} pending companies from the queue.")
        return pending_companies
    except Exception as e:
        print(f"Error retrieving pending companies: {e}")
        return []

__all__ = ["db", "connect_mongo", "store_metadata_batch", "get_pending_companies"]


if __name__ == "__main__":
    
    # Test retrieving pending companies
    pending_companies = get_pending_companies()
    print(f"Pending companies: {len(pending_companies)}")