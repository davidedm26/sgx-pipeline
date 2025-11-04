from pathlib import Path
import sys
ROOT_PATH = Path(__file__).resolve().parent.parent.parent
SRC_PATH = ROOT_PATH / "src"
sys.path.append(str(ROOT_PATH))
sys.path.append(str(SRC_PATH))

from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from config.settings import MONGODB_URI, MONGODB_DATABASE, COMPANIES_QUEUE_COLLECTION, PUBLIC_DOCUMENTS_COLLECTION, COMPANIES_UAT_COLLECTION, COMPANIES_PROD_COLLECTION, PROD_MODE 

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

    # Work on a deep copy so the original companylist passed by the caller is not mutated
    from copy import deepcopy
    local_companylist = deepcopy(companylist)

    print(f"Passed {len(local_companylist)} entries to the queue inserting function.")
    db = connect_mongo()
    if db is None:
        raise ValueError("Database connection error.")

    collection = db[COMPANIES_QUEUE_COLLECTION]
    from datetime import datetime, timezone
    current_timestamp = datetime.now(timezone.utc)  # Get the current UTC timestamp

    for company in local_companylist:
        company["processed"] = False
        company["status"] = "pending"
        company["updated_at"] = current_timestamp
        company["processed_company_metadata"] = False

    try:
        result = collection.insert_many(local_companylist, ordered=False)
        print(f"Inserted {len(result.inserted_ids)} companies into the queue.")
    except BulkWriteError as bwe:
        duplicate_count = sum(1 for error in bwe.details.get("writeErrors", []) if error.get("code") == 11000)
        added_count = len(local_companylist) - duplicate_count
        if added_count > 0:
            print(f"Inserted {added_count} new companies into the queue.")
        if duplicate_count > 0:
            print(f"{duplicate_count} companies already exist in the queue. Skipping duplicates.")
    except Exception as e:
        print(f"Error inserting companies: {e}")

def store_company_documents(companylist):
    # Implement the logic to store the company documents in the database
    if not companylist:
        print("No companies to store.")
        return

    db = connect_mongo()
    if db is None:
        raise ValueError("Database connection error.")

    for coll in [COMPANIES_UAT_COLLECTION , COMPANIES_PROD_COLLECTION if PROD_MODE else COMPANIES_UAT_COLLECTION ]:
        print(f"Passed {len(companylist)} entries to the inserting function for {coll}.")
        
        collection = db[coll]
        from datetime import datetime, timezone
        current_timestamp = datetime.now(timezone.utc)  # Get the current UTC timestamp

        for company in companylist:
            company["processed"] = False
            company["updated_at"] = current_timestamp
            
            

        try:
            result = collection.insert_many(companylist, ordered=False)
            print(f"Inserted {len(result.inserted_ids)} companies into the {coll} collection.")
        except BulkWriteError as bwe:
            #print(f"Bulk write error details: {bwe.details}")
            duplicate_count = sum(1 for error in bwe.details.get("writeErrors", []) if error.get("code") == 11000)
            added_count = len(companylist) - duplicate_count
            if added_count > 0:
                print(f"Inserted {added_count} new companies into the {coll} collection.")
            if duplicate_count > 0:
                print(f"{duplicate_count} companies already exist in the {coll} collection. Skipping duplicates.")
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
    
def reset_error_companies():
    # Implement the logic to reset companies with 'error' status back to 'pending'
    db = connect_mongo()
    if db is None:
        raise ValueError("Database connection error.")

    collection = db[COMPANIES_QUEUE_COLLECTION]
    from datetime import datetime, timezone
    current_timestamp = datetime.now(timezone.utc)  # Get the current UTC timestamp

    try:
        result = collection.update_many(
            {"status": {"$in": ["error", "cancelled"]}},
            {"$set": {
                "status": "pending",
                "updated_at": current_timestamp
            }}
        )
        print(f"Reset {result.modified_count} companies to 'pending' status.")
    except Exception as e:
        print(f"Error resetting error companies: {e}")
        raise e    

def get_companies_without_metadata():
    # Implement the logic to retrieve companies without metadata from the database
    db = connect_mongo()
    if db is None:
        raise ValueError("Database connection error.")

    collection = db[COMPANIES_QUEUE_COLLECTION]
    try:
        companies = list(collection.find({"processed_company_metadata": False}))
        print(f"Retrieved {len(companies)} companies without metadata from the queue.")
        return companies
    except Exception as e:
        print(f"Error retrieving companies without metadata: {e}")
        return []


def update_company(company_id, processed=False, status="success"):
    # Implement the logic to update the company status in the database and update UAT collection
    db = connect_mongo()
    if db is None:
        raise ValueError("Database connection error.")

    queue_coll = db[COMPANIES_QUEUE_COLLECTION]
    docs_coll = db[PUBLIC_DOCUMENTS_COLLECTION]
    
    company_doc_coll = [ db[COMPANIES_UAT_COLLECTION] , db[COMPANIES_PROD_COLLECTION] if PROD_MODE else db[COMPANIES_UAT_COLLECTION] ]
    
    from datetime import datetime, date, timezone
    current_timestamp = datetime.now(timezone.utc)  # Get the current UTC timestamp

    try:
        # Count number of documents in PUBLIC_DOCUMENTS_COLLECTION for this company_id
        num_filings = docs_coll.count_documents({"company_id": company_id})

        # Update queue collection
        result = queue_coll.update_one(
            {"company_id": company_id},
            {"$set": {
                "processed": processed,
                "status": status,
                "updated_at": current_timestamp,
                "num_filings": int(num_filings)
            }}
        )
        if result.matched_count > 0:
            print(f"Updated company {company_id} in queue to status '{status}'")
        else:
            print(f"No company found with company_id {company_id} in queue.")
            return
        
        if ( status != "success" ):
            # If not successful, skip updating UAT/PROD collections
            return
        
         # Determine latest filing date (if any) from PUBLIC_DOCUMENTS_COLLECTION
        latest_doc = docs_coll.find_one({"company_id": company_id}, sort=[("filing_date", -1)])
        latest_filing_date_str = None
        if latest_doc:
            fd = latest_doc.get("filing_date")
            if isinstance(fd, datetime):
                latest_filing_date_str = fd.date().isoformat()
            elif isinstance(fd, date):
                latest_filing_date_str = fd.isoformat()
            elif isinstance(fd, str):
                # try to parse ISO-like strings, fallback to first 10 chars
                try:
                    parsed = datetime.fromisoformat(fd)
                    latest_filing_date_str = parsed.date().isoformat()
                except Exception:
                    if len(fd) >= 10:
                        latest_filing_date_str = fd[:10]
                    else:
                        latest_filing_date_str = fd

        #s3_path = latest_doc.get("file_path") if latest_doc else None
        
        '''
        if s3_path:
            if ('\\' in s3_path):
                s3_path =  "\\".join(s3_path.split('\\')[:2])
            elif ('/' in s3_path):
                s3_path =  "/".join(s3_path.split('/')[:2])
            else:
                s3_path = None
        '''
        
        from utils.path_utils import convert_path_to_linux_format
        s3_path = latest_doc.get("file_path") if latest_doc else None
        if s3_path:
            s3_path = convert_path_to_linux_format(s3_path)

        # Update UAT/PROD company collection: set processed and latest_filing_date (and updated_at)
        for coll in company_doc_coll:
            company_doc_result = coll.update_one(
            {"company_id": company_id},
            {"$set": {
                "processed": processed,
                "latest_filing_date": latest_filing_date_str,
                "updated_at": current_timestamp,
                "s3_path": s3_path,
                "num_filings": int(num_filings)
            }}
        )
        if company_doc_result.matched_count > 0:
            print(f"Updated company_id {company_id} in UAT/PROD collection: processed={processed}, latest_filing_date={latest_filing_date_str}.")
            print ("#" * 80)
        else:
            print(f"No company found with company_id {company_id} in UAT collection.")

    except Exception as e:
        print(f"Error updating company status for company_id {company_id}: {e}")

def update_company_metadata(company_id, metadata):
    """
    Update the company document in UAT/PROD collection by appending metadata,
    and set processed_company_metadata=True in the queue collection.
    """
    db = connect_mongo()
    if db is None:
        raise ValueError("Database connection error.")

    # Choose UAT and PROD collections
    collections = [db[COMPANIES_UAT_COLLECTION]]
    if PROD_MODE:
        collections.append(db[COMPANIES_PROD_COLLECTION])

    # Update metadata in UAT/PROD collections
    for coll in collections:
        result = coll.update_one(
            {"company_id": company_id},
            {"$set": {"metadata": metadata}}
        )
        if result.matched_count > 0:
            print(f"Appended metadata to company_id {company_id} in {coll.name}.")
        else:
            print(f"No company found with company_id {company_id} in {coll.name}.")

    # Update processed_company_metadata in queue collection
    queue_coll = db[COMPANIES_QUEUE_COLLECTION]
    from datetime import datetime, timezone
    queue_result = queue_coll.update_one(
        {"company_id": company_id},
        {"$set": {
            "processed_company_metadata": True,
            "metadata_updated_at": datetime.now(timezone.utc)
        }}
    )
    if queue_result.matched_count > 0:
        print(f"Set processed_company_metadata=True for company_id {company_id} in queue.")
    else:
        print(f"No company found with company_id {company_id} in queue.")
    

__all__ = ["db", "connect_mongo", "store_metadata_batch", "get_pending_companies"]


if __name__ == "__main__":
    
    # Test retrieving pending companies
    pending_companies = get_pending_companies()
    print(f"Pending companies: {len(pending_companies)}")
    
    update_company(2995, processed=True, status="processed")
    