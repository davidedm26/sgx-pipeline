from pathlib import Path
import sys
ROOT_PATH = Path(__file__).resolve().parent.parent.parent
SRC_PATH = ROOT_PATH / "src"
sys.path.append(str(ROOT_PATH))
sys.path.append(str(SRC_PATH))

from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import datetime
from datetime import datetime, timezone
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
            [("company_id", 1)], unique=True, background=True
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
        #db[COMPANIES_PROD_COLLECTION].create_index(
        #    [("name", 1)], unique=True, background=True
        #)

        db[COMPANIES_UAT_COLLECTION].create_index(
            [("company_id", 1)], unique=True, background=True
        )
        #db[COMPANIES_UAT_COLLECTION].create_index(
        #    [("name", 1)], unique=True, background=True
        #)

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

    if db is None:
        db = connect_mongo()

    if not metadata_list:
        print("No metadata to store.")
        return
    
    from pymongo.errors import DuplicateKeyError
    from pathlib import Path
    
    collection = db[PUBLIC_DOCUMENTS_COLLECTION]
    try:
        result = collection.insert_many(metadata_list, ordered=False)
        print(f"Saved {len(result.inserted_ids)} documents in {PUBLIC_DOCUMENTS_COLLECTION}.")
    except BulkWriteError as bwe:

        write_errors = bwe.details.get("writeErrors", []) or []
        # Count duplicates only for file_name/document_id patterns
        duplicate_count = sum(
            1
            for error in write_errors
            if error.get("code") == 11000
            and any(k in (error.get("keyPattern") or {}) for k in ("file_name", "document_id"))
        )
        added_count = len(metadata_list) - duplicate_count
        if added_count > 0:
            print(f"Saved {added_count} new documents in {PUBLIC_DOCUMENTS_COLLECTION}.")
        if duplicate_count > 0:
            print(f"{duplicate_count} documents already exist in the collection or need to be renamed.")

        # Handle each duplicate error individually
        for error in write_errors:
            if error.get("code") != 11000:
                continue

            idx = error.get("index")
            if idx is None or idx < 0 or idx >= len(metadata_list):
                # Can't resolve which document caused the error
                print(f"Skipping unresolved duplicate error: {error}")
                continue

            duplicate_file = dict(metadata_list[idx])  # copy to avoid mutating original list

            key_pattern = error.get("keyPattern") or {}
            # Normalize: keyPattern can be dict-like with int values
            if "document_id" in key_pattern:
                # Handle duplicate based on document_id: prefer the doc with more supporting_file_paths
                doc_id = duplicate_file.get("document_id", "unknown")
                existing_doc = collection.find_one({"document_id": doc_id})
                if existing_doc:
                    new_supporting = duplicate_file.get("supporting_file_paths") or []
                    existing_supporting = existing_doc.get("supporting_file_paths") or []
                    if len(new_supporting) > len(existing_supporting):
                        try:
                            # Exclude immutable _id
                            duplicate_file_without_id = {k: v for k, v in duplicate_file.items() if k != "_id"}
                            collection.update_one({"document_id": doc_id}, {"$set": duplicate_file_without_id})
                            print(f"Updated document_id {doc_id} with more complete supporting_file_paths.")
                        except Exception as e:
                            print(f"Failed to update document_id {doc_id}: {e}")
                    else:
                        #print(f"Existing document_id {doc_id} has equal or more supporting paths. Skipping update.")
                        pass
                else:
                    print(f"Duplicate document_id reported but no existing doc found for id={doc_id}. Skipping.")

            elif "file_name" in key_pattern:
                # Handle duplicate file_name by generating a new unique filename preserving extension
                base_name = duplicate_file.get("file_name", "unknown")
                p = Path(base_name)
                stem = p.stem
                suffix = p.suffix or ""
                counter = 1
                new_file_name = f"{stem} {suffix}[{counter}]"
                # Ensure we don't loop indefinitely; set a reasonable max attempts
                max_attempts = 10000
                attempts = 0
                while collection.find_one({"file_name": new_file_name}):
                    attempts += 1
                    if attempts >= max_attempts:
                        print(f"Failed to find unique filename for {base_name} after {attempts} attempts. Skipping.")
                        new_file_name = None
                        break
                    counter += 1
                    new_file_name = f"{stem} {suffix}[{counter}]"

                if not new_file_name:
                    continue

                # Prepare doc for insertion: remove _id to avoid duplicate ObjectId issues
                doc_to_insert = {k: v for k, v in duplicate_file.items() if k != "_id"}
                doc_to_insert["file_name"] = new_file_name

                # Before inserting, ensure we don't collide on other unique keys (e.g., document_id)
                doc_id = doc_to_insert.get("document_id")
                if doc_id:
                    existing_doc = collection.find_one({"document_id": doc_id})
                    if existing_doc:
                        # Existing document with same document_id found. Decide resolution based on supporting files.
                        new_supporting = doc_to_insert.get("supporting_file_paths") or []
                        existing_supporting = existing_doc.get("supporting_file_paths") or []
                        if len(new_supporting) > len(existing_supporting):
                            try:
                                # Update existing document with the more complete metadata from the renamed file
                                duplicate_file_without_id = {k: v for k, v in doc_to_insert.items() if k != "_id"}
                                collection.update_one({"document_id": doc_id}, {"$set": duplicate_file_without_id})
                                print(f"Updated existing document_id {doc_id} with more complete supporting_file_paths (from renamed file).")
                            except Exception as e:
                                print(f"Failed to update existing document {doc_id}: {e}")
                        else:
                            print(f"Existing document with document_id {doc_id} already present; skipping insertion of renamed file {new_file_name}.")
                        # In either case we've handled the document_id conflict; skip attempting to insert
                        continue

                try:
                    collection.insert_one(doc_to_insert)
                    print(f"Inserted duplicate with new name: {new_file_name}")
                except DuplicateKeyError as dke:
                    # If still duplicate (race condition or other unique key), log and skip
                    print(f"Could not insert renamed file {new_file_name} due to duplicate key: {dke}")
                except Exception as e:
                    print(f"Failed to insert renamed file {new_file_name}: {e}")

            else:
                # Unknown unique key causing 11000 - report for further investigation
                print(f"Unhandled duplicate key pattern in error: {error.get('keyPattern')} for index {idx}")
    except Exception as e:
        print(f"Batch insert error: {e}")

def store_company_queue(companylist, default_status: str = "pending"):
    # Implement the logic to store the company queue in the database
    if not companylist:
        print("No companies to store.")
        return

    # Work on a deep copy so the original companylist passed by the caller is not mutated
    from copy import deepcopy
    local_companylist = deepcopy(companylist)

    print(f"Passed {len(local_companylist)} entries to the queue inserting function with status '{default_status}'.")
    db = connect_mongo()
    if db is None:
        raise ValueError("Database connection error.")

    collection = db[COMPANIES_QUEUE_COLLECTION]
    from datetime import datetime, timezone
    current_timestamp = datetime.now(timezone.utc)  # Get the current UTC timestamp

    inserted = 0
    skipped_conflicts = 0

    for comp in local_companylist:
        #name = comp.get("name")
        cid = comp.get("company_id")

        # Check if a document with the same company_id already exists
        try:
            existing = collection.find_one({"company_id": cid})
        except Exception as e:
            print(f"DB lookup error for company_id='{cid}': {e}")
            existing = None

        if existing:
            # Conflict if company_id differs
            if str(existing.get("company_id")) != str(cid):
                skipped_conflicts += 1
                print(f"Skipping insert for company_id='{cid}' because an existing entry has company_id={existing.get('company_id')} (incoming {cid}).")
                continue
            else:
                # Already present with same company_id: update timestamps
                try:
                    collection.update_one({"company_id": cid}, {"$set": {"updated_at": current_timestamp}})
                except Exception as e:
                    print(f"Failed to update timestamp for existing company '{cid}': {e}")
                continue

        # Not existing: prepare document and insert
        doc = dict(comp)
        doc.setdefault("processed", False)
        doc["status"] = default_status
        doc["updated_at"] = current_timestamp
        doc.setdefault("processed_company_metadata", False)

        try:
            collection.insert_one(doc)
            inserted += 1
        except Exception as e:
            print(f"Failed to insert company '{cid}': {e}")

    print(f"Inserted {inserted} new companies into the queue with status '{default_status}'.")
    if skipped_conflicts > 0:
        print(f"Skipped {skipped_conflicts} companies due to name/company_id conflicts.")

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
        print(f"Retrieved {len(pending_companies)} pending companies from the queue collection.")
        return pending_companies
    except Exception as e:
        print(f"Error retrieving pending companies: {e}")
        return []
    
#This function returns all the company in the queue
def get_companies_to_ticker():
    # Implement the logic to retrieve pending companies from the database
    db = connect_mongo()
    if db is None:
        raise ValueError("Database connection error.")

    collection = db[COMPANIES_QUEUE_COLLECTION]
    try:
        companies = list(collection.find({"processed_company_metadata": True}))
        print(f"Retrieved {len(companies)} companies from the queue collection.")
        return companies
    except Exception as e:
        print(f"Error retrieving companies: {e}")
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
            {"status": {"$in": ["error", "cancelled", "running"]}},
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
        print(f"Retrieved {len(companies)} companies without metadata from the queue collection.")
        return companies
    except Exception as e:
        print(f"Error retrieving companies without metadata: {e}")
        return []

def get_queue_company_list():
    # Return list of (company_id) pairs for companies in the queue
    db = connect_mongo()
    if db is None:
        raise ValueError("Database connection error.")

    collection = db[COMPANIES_QUEUE_COLLECTION]
    try:
        cursor = collection.find({}, {"company_id": 1})
        companies = [doc.get("company_id") for doc in cursor]
        print(f"CHECKING QUEUE COLLECTION TO FOUND EXISTING COMPANIES: Found a total of {len(companies)} companies in the queue collection.")
        return companies
    except Exception as e:
        print(f"Error retrieving companies from the queue: {e}")
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
            #get only the first two segments of the path
            s3_path = convert_path_to_linux_format(s3_path)
            s3_path = "/".join(s3_path.split('/')[:3]) + "/"

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
        
def add_company_name(company_id, company_name):
    # Implement the logic to add company_name for the company in the queue collection
    db = connect_mongo()
    if db is None:
        raise ValueError("Database connection error.")

    queue_coll = db[COMPANIES_QUEUE_COLLECTION]
    try:
        # Update queue collection
        result = queue_coll.update_one(
            {"company_id": company_id},
            {"$set": {
                "name": company_name,
                "updated_at": datetime.now(timezone.utc)
            }}
        )
        if result.matched_count > 0:
            #print(f"Added/Updated company_name for company_id {company_id} in queue to '{company_name}'")
            pass
        else:
            print(f"No company found with company_id {company_id} in queue to add/update name.")
            
        #Update prod/uat
        for coll in [ db[COMPANIES_UAT_COLLECTION] , db[COMPANIES_PROD_COLLECTION] if PROD_MODE else db[COMPANIES_UAT_COLLECTION] ]:
            company_doc_result = coll.update_one(
                {"company_id": company_id},
                {"$set": {
                    "name": company_name,
                    "updated_at": datetime.now(timezone.utc)
                }}
            )
            if company_doc_result.matched_count > 0:
                #print(f"Added/Updated company_name for company_id {company_id} in {coll.name} to '{company_name}'")
                pass
            else:
                print(f"No company found with company_id {company_id} in {coll.name} to add/update name.")
        
    except Exception as e:
        print(f"Error adding/updating company name for company_id {company_id}: {e}")

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
            #print(f"Appended metadata to company_id {company_id} in {coll.name}.")
            pass
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
        #print(f"Set processed_company_metadata=True for company_id {company_id} in queue.")
        pass
    else:
        print(f"No company found with company_id {company_id} in queue.")
        
        
        

def add_ticker_info(company_id, ticker_code, ticker_name):
    # Add/update ticker_code and ticker_name for the company in queue and UAT/PROD collections
    db = connect_mongo()
    if db is None:
        raise ValueError("Database connection error.")

    updated_at = datetime.now(timezone.utc)

    # Update queue collection so the queue reflects ticker info
    queue_coll = db[COMPANIES_QUEUE_COLLECTION]
    try:
        qres = queue_coll.update_one(
            {"company_id": company_id},
            {"$set": {"metadata_updated_at": updated_at}}
        )
        if qres.matched_count > 0:
            print(f"Added/Updated metadata_updated_at for company_id {company_id}")
        else:
            print(f"No company found with company_id {company_id} in queue to add/update ticker.")
    except Exception as e:
        print(f"Error updating ticker in queue for company_id {company_id}: {e}")

    # Update UAT and optionally PROD collections
    for coll in [db[COMPANIES_UAT_COLLECTION], db[COMPANIES_PROD_COLLECTION] if PROD_MODE else db[COMPANIES_UAT_COLLECTION]]:
        try:
            res = coll.update_one(
                {"company_id": company_id},
                {"$set": {
                    "metadata.code": ticker_code,
                    "metadata.trading_name": ticker_name
                }}
            )
            if res.matched_count > 0:
                print(f"Added/Updated ticker for company_id {company_id} in {coll.name}: {ticker_code} / {ticker_name}")
            else:
                print(f"No company found with company_id {company_id} in {coll.name} to add/update ticker.")
        except Exception as e:
            print(f"Error updating ticker in {coll.name} for company_id {company_id}: {e}")


    
    

__all__ = ["db", "connect_mongo", "store_metadata_batch", "get_pending_companies"]


if __name__ == "__main__":
    
    # Test retrieving pending companies
    pending_companies = get_pending_companies()
    print(f"Pending companies: {len(pending_companies)}")
    
    update_company(2995, processed=True, status="success")
    