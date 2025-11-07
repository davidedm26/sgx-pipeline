from pathlib import Path
import sys
ROOT_PATH = Path(__file__).resolve().parent.parent.parent
SRC_PATH = ROOT_PATH / "src"
sys.path.append(str(ROOT_PATH))
sys.path.append(str(SRC_PATH))

from tenacity import retry, stop_after_attempt, wait_exponential
from config.settings import MAX_RETRIES, BACKOFF_FACTOR, REQUEST_TIMEOUT, COMPANYLIST_URL , CORPORATEINFO_URL , COMPANY_PAGE_SIZE, COMPANY_MAX_PAGES, MAX_WORKERS, PERIOD_END, MAX_COMPANIES, PERIOD_START 
import requests
from utils.http_requests_utils import get_headers
from utils.scraping_utils import get_search_results, extract_documents_list 


def get_company_result_dict():
    # Implement the logic to get the company's metadata
    # Search in corporate information page
    results = get_search_results(periodstart=PERIOD_START, periodend=PERIOD_END, url=CORPORATEINFO_URL, pagesize=COMPANY_PAGE_SIZE)

    if (not results) or ("meta" not in results):
        print("Failed to retrieve search results or missing metadata.")
        raise ValueError("Invalid search results")
    
    n_pages =  results.get("meta", {}).get("totalPages", 1) # Total number of pages

    all_documents = []

    # Collect all documents from all pages
    for page_num in range(min(n_pages, COMPANY_MAX_PAGES) if COMPANY_MAX_PAGES > 0 else n_pages):
        #if shutdown_event.is_set():
        #    print("Shutdown event detected. Exiting document collection loop.")
        #    return

        #print(f"Processing page {page_num + 1}/{n_pages} (page size {COMPANY_PAGE_SIZE}) for company search")

        response = get_search_results(periodstart=PERIOD_START, periodend=PERIOD_END, url=CORPORATEINFO_URL, pagesize=COMPANY_PAGE_SIZE, pagestart=page_num)

        if not response:
            print(f"Failed to retrieve search results for company search on page {page_num + 1}")
            continue

        doc_list = extract_documents_list(response)
        if doc_list:
            all_documents.extend(doc_list)
            '''
            if (len(all_documents) >= MAX_FILES_PER_COMPANY) and (MAX_FILES_PER_COMPANY > 0):
                print(f"Reached maximum documents per company limit: {MAX_FILES_PER_COMPANY}")
                all_documents = all_documents[:MAX_FILES_PER_COMPANY]
                break
            '''
            #print(f"Collected {len(doc_list)} documents from page {page_num + 1}")
            #print(f"Total documents collected so far: {len(all_documents)}")
        else:
            print(f"No documents found on page {page_num + 1}")
            continue
        
    print(f"Total documents collected: {len(all_documents)}")
    
    return all_documents[:MAX_COMPANIES] if MAX_COMPANIES > 0 else all_documents
    


@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=BACKOFF_FACTOR, min=1, max=REQUEST_TIMEOUT))
def get_json_response(url):
    # Implement the logic to retrieve the list of company names
    response = requests.get(url, headers=get_headers(), timeout=10)
    response.raise_for_status()
    return response.json()




if __name__ == "__main__":
    '''
    company_dict = get_company_result_dict()
    print(f"Scraped metadata for {len(company_dict)} documents.")
    # Save metadata to a JSON file for debugging
    debug_file_path = ROOT_PATH / "debug" / "debug_metadata.json"
    try:
        with open(debug_file_path, "w", encoding="utf-8") as debug_file:
            import json
            json.dump(company_dict, debug_file, indent=4, ensure_ascii=False)
        print(f"Metadata successfully saved to {debug_file_path}")
    except Exception as e:
        print(f"Error saving metadata to JSON file: {e}")
    '''
    #populate_company_collections()