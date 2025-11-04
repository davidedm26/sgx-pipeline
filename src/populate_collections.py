import sys
from pathlib import Path
ROOT_PATH = Path(__file__).resolve().parent.parent
SRC_PATH = ROOT_PATH / "src"
sys.path.append(str(ROOT_PATH))
sys.path.append(str(SRC_PATH))



def populate_company_collections():
    # Implement the logic to populate the queue with company names
    from utils.company_metadata_utils import get_company_result_dict
    company_dict = get_company_result_dict()
    if company_dict:
        # Extract and print the list of document company_names for debugging
        name_code_dict = [{"name": doc.get("companyName", "Unknown Name"), "company_id": doc.get("id", "Unknown Code")} for doc in company_dict]
        #print(f"Document Company Data: {name_code_dict}")
        
        from utils.db_utils import store_company_queue
        try:
            #print(name_code_dict)
            store_company_queue(companylist=name_code_dict)
        except Exception as e:
            print(f"Error storing company queue: {e}")
        try:
            from utils.db_utils import store_company_documents
            #print(name_code_dict)
            store_company_documents(companylist=name_code_dict)
        except Exception as e:
            print(f"Error storing company collections: {e}")
    else:
        raise ValueError("No company data retrieved.")
    

if __name__ == "__main__":
    populate_company_collections()