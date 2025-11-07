import sys
from pathlib import Path
ROOT_PATH = Path(__file__).resolve().parent.parent
SRC_PATH = ROOT_PATH / "src"
sys.path.append(str(ROOT_PATH))
sys.path.append(str(SRC_PATH))

from config.settings import COMPANY_LIST_URL 

'''
def populate_company_collections():
    # Implement the logic to populate the queue with company names
    from utils.company_metadata_utils import get_company_result_dict
    company_dict = get_company_result_dict()
    if company_dict:        
        # Extract and print the list of document company_names for debugging
        name_code_dict = [{"name": doc.get("companyName", "Unknown Name"), "company_id": doc.get("id", "Unknown Code")} for doc in company_dict]
        #print(f"Document Company Data: {name_code_dict}")
        
        #esclude from the matching algorithm the already queued companies
        from utils.db_utils import get_queue_company_list 
        in_queue_company_list = get_queue_company_list() #returns list of (name, company_id) tuples
        if (in_queue_company_list) and (len(in_queue_company_list) > 0):
            original_count = len(name_code_dict)
            # normalize and create sets for quick  checks
            queued_names = {t[0].strip().lower() for t in in_queue_company_list}
            queued_ids = {str(t[1]).strip() for t in in_queue_company_list}

            filtered = [
                entry for entry in name_code_dict
                if (entry.get("name", "").strip().lower() not in queued_names
                    and str(entry.get("company_id", "")).strip() not in queued_ids)
            ]
            excluded_count = original_count - len(filtered)
            name_code_dict = filtered
            print(f"Excluding {excluded_count} companies already in queue. Remaining companies to process: {len(name_code_dict)}")
        
        dict = match_company_names(listed_company=name_code_dict)
        print(f"Matched Company Data: {dict}")
        
        #generate new name_code_dict with matched names
        name_code_dict = [{"name": entry.get("matched_name"), "company_id": entry.get("company_id")} for entry in dict]
        
        from utils.db_utils import store_company_queue
        try:
            #print(name_code_dict)
            store_company_queue(companylist=name_code_dict) #populate queue
        except Exception as e:
            print(f"Error storing company queue: {e}")
        try:
            from utils.db_utils import store_company_documents
            #print(name_code_dict)
            store_company_documents(companylist=name_code_dict) #populate uat/prod
        except Exception as e:
            print(f"Error storing company collections: {e}")
    else:
        raise ValueError("No company data retrieved.")
'''

'''
def populate_company_collections():
    # Implement the logic to populate the queue with company names
    from utils.company_metadata_utils import get_company_result_dict
    company_listed = get_company_result_dict()
    if company_listed:        
        # Extract and print the list of document company_names for debugging
        listed_company_info = [{"name": doc.get("companyName", "Unknown Name"), "company_id": doc.get("id", "Unknown Code")} for doc in company_listed]
        #print(f"Document Company Data: {name_code_dict}")
        
        #esclude from the matching algorithm the already queued companies
        from utils.db_utils import get_queue_company_list 
        in_queue_company = get_queue_company_list() #returns list of (name, company_id) tuples
        #print(f"Companies already in queue: {in_queue_company}")
        
        if (in_queue_company) and (len(in_queue_company) > 0):
            original_count = len(listed_company_info)
            # normalize and create sets for quick  checks
            #queued_names = {t[0].strip().lower() for t in in_queue_company}
            #get all the ids in the queue
            queued_ids = {str(t[1]).strip() for t in in_queue_company}

            #take only those entries whose company_id is not in the queued_ids
            new_company_info = [
                entry for entry in listed_company_info
                if (str(entry.get("company_id", "")).strip() not in queued_ids)
            ]
            
            #count how many were excluded            
            excluded_count = original_count - len(new_company_info)
            print(f"Excluding {excluded_count} companies already in queue. Remaining companies to process: {len(new_company_info)}")
        
        else:
            new_company_info = listed_company_info

        matched_company_dict = match_company_names(listed_company=new_company_info)
        print(f"Matched Company Data: {matched_company_dict}")
        
        #if some companies has the same name but different company_id, we need to handle that
        # take the one that comes first in the list (higher priority) and extract the others (they will be appended to unmatched)
        
                
                

        #generate new name_code_dict with matched names
        inserting_company = [{"name": entry.get("matched_name"), "company_id": entry.get("company_id")} for entry in matched_company_dict if entry.get("confidence", 0) >= 90.0]

        #print(f"Companies to be inserted (confidence >= 90.0): {inserting_company}")
        from utils.db_utils import store_company_queue
        try:
            #print(name_code_dict)
            store_company_queue(companylist=inserting_company) #populate queue
        except Exception as e:
            print(f"Error storing company queue: {e}")
            
        # Persist low-confidence / unmatched entries so they are not reprocessed repeatedly
        unmatched_company = [
            {"name": entry.get("original_name") or entry.get("matched_name"), "company_id": entry.get("company_id")}
            for entry in matched_company_dict
            if entry.get("confidence", 0) < 90.0
        ]
        if unmatched_company:
            try:
                store_company_queue(companylist=unmatched_company, default_status="unmatched")
                print(f"Stored {len(unmatched_company)} unmatched/low-confidence companies with status 'unmatched'.")
            except Exception as e:
                print(f"Error storing unmatched companies: {e}")
        try:
            from utils.db_utils import store_company_documents
            #print(name_code_dict)
            store_company_documents(companylist=inserting_company) #populate uat/prod
        except Exception as e:
            print(f"Error storing company collections: {e}")
    else:
        raise ValueError("No company data retrieved.")
'''

'''
def match_company_names(listed_company):
    """_summary_

    Args:
        name_code_dict (list): A list of dictionaries containing matched company names and IDs.

    Returns:
        list: A list of matched company names with their confidence scores.
    """
    from utils.string_matching_utils import get_label_and_confidence
    import re

    # Get the listed company names and their token count dictionary
    all_company, count_dict = get_company_name_list_and_count_dict()
    
    #remove from all_company any name that is present in the queue and update count_dict
    from utils.db_utils import get_queue_company_list
    in_queue_company_list = get_queue_company_list() #returns list of (name, company_id) tuples
    if (in_queue_company_list) and (len(in_queue_company_list) > 0):
        queued_names = {t[0].strip().lower() for t in in_queue_company_list}
        original_count = len(all_company)
        all_company = [name for name in all_company if name.strip().lower() not in queued_names]
        excluded_count = original_count - len(all_company)
        # update count_dict by removing tokens of excluded names
        for name in queued_names:
            tokens = re.findall(r"\w+", (name or "").lower())
            for tok in tokens:
                if tok in count_dict:
                    count_dict[tok] -= 1
                    if count_dict[tok] <= 0:
                        del count_dict[tok]
                        
        print(f"Removed {excluded_count} company names already in queue from candidate pool. Remaining candidates: {len(all_company)}")
    
    results = []

    # First pass: split exact matches and candidates to run fuzzy matching on
    exact_matches = []
    to_match = []
    
    for entry in listed_company:
        company_name = entry.get("name", "")
        if company_name in all_company:
            exact_matches.append({
                "original_name": company_name,
                "matched_name": company_name,
                "matched_source": company_name,
                "confidence": 101,
                "company_id": entry.get("company_id", "Unknown Code")
            })
        else:
            to_match.append(entry)

    # Add exact matches first
    results.extend(exact_matches)

    # Provide quick feedback about exact matches
    if exact_matches:
        print(f"Exact matches found: {len(exact_matches)}")
        for m in exact_matches[:5]:  # Print first 5 exact matchess
            print(f"  {m['original_name']} -> {m['matched_name']}")
            
            
    # remove names already matched from all_company and update count_dict
    if exact_matches:
        removed = {m.get("matched_name") for m in exact_matches if m.get("matched_name")}
        if removed:
            # filter candidates excluding already matched names
            all_company = [n for n in all_company if n not in removed]
            # update token counts by subtracting tokens of removed entries
            for name in removed:
                tokens = re.findall(r"\w+", (name or "").lower())
                for tok in tokens:
                    if tok in count_dict:
                        count_dict[tok] -= 1
                        if count_dict[tok] <= 0:
                            del count_dict[tok]
            print(f"Removed {len(removed)} exact-match company names from candidate pool")

    # Second pass: fuzzy matching with progress bar
    from tqdm import tqdm
    SINGLE_TOKEN_THRESHOLD = 3
    if to_match:
        for entry in tqdm(to_match, desc="String matching", unit="company"):
            company_name = entry.get("name", "")
            best_label = None
            best_confidence = 0
            best_source = None
            for checking_name in all_company:
                try:
                    label, confidence = get_label_and_confidence(company_name, checking_name, count_dict, SINGLE_TOKEN_THRESHOLD=SINGLE_TOKEN_THRESHOLD)
                except Exception:
                    # If the helper fails for a candidate, skip it
                    continue
                if confidence is None:
                    continue
                if confidence == 100 or confidence == 100.0:
                    best_label = checking_name
                    best_confidence = confidence
                    best_source = checking_name
                    continue
                if confidence > best_confidence:
                    best_confidence = confidence
                    best_label = checking_name
                    best_source = checking_name
            if best_label is None:
                best_label = f"{company_name} (unmatched)"
                best_confidence = 0.0
            results.append({
                "original_name": company_name,
                "matched_name": best_label,
                "matched_source": best_source,
                "confidence": best_confidence,
                "company_id": entry.get("company_id", "Unknown Code")
            })
            
            #remove the company name from all_company and update count_dict
            if best_source in all_company:
                all_company.remove(best_source)
                tokens = re.findall(r"\w+", (best_source or "").lower())
                for tok in tokens:
                    if tok in count_dict:
                        count_dict[tok] -= 1
                        if count_dict[tok] <= 0:
                            del count_dict[tok]
    
    return results
'''

def populate_company_collections():
    # Implement the logic to populate the queue with company names
    from utils.company_metadata_utils import get_company_result_dict
    company_listed = get_company_result_dict()
    if company_listed:        
        # Extract and print the list of document company_names for debugging
        listed_company_info = [{"company_id": doc.get("id", "Unknown Code")} for doc in company_listed]
        #print(f"Document Company Data: {name_code_dict}")
        
        #esclude from the matching algorithm the already queued companies
        from utils.db_utils import get_queue_company_list 
        in_queue_company = get_queue_company_list() #returns list of (name, company_id) tuples
        #print(f"Companies already in queue: {in_queue_company}")
        
        if (in_queue_company) and (len(in_queue_company) > 0):
            original_count = len(listed_company_info)
            # normalize and create sets for quick  checks
            #queued_names = {t[0].strip().lower() for t in in_queue_company}
            #get all the ids in the queue
            queued_ids = {str(t).strip() for t in in_queue_company}

            #take only those entries whose company_id is not in the queued_ids
            new_company_info = [
                entry for entry in listed_company_info
                if (str(entry.get("company_id", "")).strip() not in queued_ids)
            ]
            
            #count how many were excluded            
            excluded_count = original_count - len(new_company_info)
            print(f"Excluding {excluded_count} companies already in queue. Remaining companies to process: {len(new_company_info)}")
        
        else:
            new_company_info = listed_company_info


        #print(f"Companies to be inserted (confidence >= 90.0): {inserting_company}")
        from utils.db_utils import store_company_queue
        try:
            #print(name_code_dict)
            store_company_queue(companylist=new_company_info) #populate queue
        except Exception as e:
            print(f"Error storing company queue: {e}")
        
        try:
            from utils.db_utils import store_company_documents
            #print(name_code_dict)
            store_company_documents(companylist=new_company_info) #populate uat/prod
        except Exception as e:
            print(f"Error storing company collections: {e}")
    else:
        raise ValueError("No company data retrieved.")


def get_company_name_list_and_count_dict():
    from utils.scraping_utils import get_web_page
    import json
    import re

    company_list_json = get_web_page(COMPANY_LIST_URL)
    if company_list_json is None:
        raise ValueError("Error retrieving company list from SGX API")
    # If the web helper returned a JSON string, parse it
    if isinstance(company_list_json, str):
        try:
            company_list_json = json.loads(company_list_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing JSON from SGX API: {e}")
    company_list = company_list_json.get("data", [])
    if (not company_list) or (len(company_list) == 0):
        raise ValueError("No company names found in SGX API response")
    # Build and return a name->count dict for downstream matching
    count_dict = {}
    for name in company_list:
        tokens = re.findall(r"\w+", (name or "").lower())
        for token in tokens:
            count_dict[token] = count_dict.get(token, 0) + 1
    #print(company_list)
    #print(count_dict)
    return company_list, count_dict


if __name__ == "__main__":
    '''
    name_code_dict = [
        {"name": "ADVANCEDSYSTEMS AUTOMATION", "company_id": "12345"},
        {"name": "PASTURE HOLDINGS LTD.", "company_id": "67890"},
        {"name": "SHANGHAI TURBO ENTERPRISES LTD.", "company_id": "67890"},

    ]

    name_code_dict_matched = match_company_names(name_code_dict)
    print(name_code_dict_matched)
    '''
    import time
    import re
    start = time.perf_counter()
    try:
        populate_company_collections()
    except Exception as e:
        elapsed = time.perf_counter() - start
        print(f"Error during populate_company_collections after {elapsed:.2f}s: {e}")
        raise
    else:
        elapsed = time.perf_counter() - start
        print(f"populate_company_collections completed in {elapsed:.2f} seconds")