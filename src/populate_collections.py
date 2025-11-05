import sys
from pathlib import Path
ROOT_PATH = Path(__file__).resolve().parent.parent
SRC_PATH = ROOT_PATH / "src"
sys.path.append(str(ROOT_PATH))
sys.path.append(str(SRC_PATH))

from config.settings import COMPANY_LIST_URL 


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
    
def match_company_names(listed_company):
    """_summary_

    Args:
        name_code_dict (list): A list of dictionaries containing matched company names and IDs.

    Returns:
        list: A list of matched company names with their confidence scores.
    """
    from utils.string_matching_utils import get_label_and_confidence

    # Get the listed company names and their token count dictionary
    all_company, count_dict = get_company_name_list_and_count_dict()
    
    #entry = name_code_dict[0]  # Process one entry at a time
    
    matched_list = []
    
    for entry in listed_company:
        company_name = entry.get("name", "")
        if (company_name in all_company):
            best_label = company_name
            best_confidence = 1.0
            best_source = company_name
            matched_list.append({
                "original_name": company_name,
                "matched_name": best_label,
                "matched_source": best_source,
                "confidence": best_confidence,
                "company_id": entry.get("company_id", "Unknown Code")
            })
            continue
        
        best_label = None
        best_confidence = -1.0
        best_source = None
        for checking_name in all_company:
            label, confidence = get_label_and_confidence(company_name, checking_name, count_dict, SINGLE_TOKEN_THRESHOLD=50)
            if confidence is None:
                continue
            if confidence > best_confidence:
                best_confidence = confidence
                # get_label_and_confidence returns (label, confidence) where
                # label is 1 (match) or 0 (no match). We must not use the
                # numeric label as the matched name. Use the checking_name
                # (the official name) when this candidate has the best score.
                best_label = checking_name
                best_source = checking_name
        if best_label is None:
            best_label = company_name + " (unmatched)"
            best_confidence = 0.0
        matched_list.append({
            "original_name": company_name,
            "matched_name": best_label,
            "matched_source": best_source,
            "confidence": best_confidence,
            "company_id": entry.get("company_id", "Unknown Code")
        })
    return matched_list

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