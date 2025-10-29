import sys
from pathlib import Path
ROOT_PATH = Path(__file__).resolve().parent.parent
SRC_PATH = ROOT_PATH / "src"
sys.path.append(str(ROOT_PATH))
sys.path.append(str(SRC_PATH))



def populate_company_queue():
    # Implement the logic to populate the queue with company names
    from utils.company_metadata_utils import get_company_result_dict
    company_dict = get_company_result_dict()
    if company_dict:
        # Extract and print the list of document company_names for debugging
        name_code_dict = [{"name": doc.get("companyName", "Unknown Name"), "company_id": doc.get("id", "Unknown Code")} for doc in company_dict]
        #print(f"Document Company Data: {name_code_dict}")

        
        from utils.db_utils import store_company_queue
        try:
            store_company_queue(name_code_dict)
        except Exception as e:
            print(f"Error storing company queue: {e}")
    else:
        raise ValueError("No company data retrieved.")
    

if __name__ == "__main__":
    populate_company_queue()