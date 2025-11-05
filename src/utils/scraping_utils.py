"""Safer scraper utilities for SGX announcements.

This module provides small helpers to call the SGX announcements API
without embedding sensitive values directly into URLs or module-level
constants that may not be defined in the runtime environment.

"""

from typing import Optional
import requests 
from requests.exceptions import RequestException 
import json as _json
import os   
# Add the src directory to the Python path to allow imports from the entire src folder
from pathlib import Path
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import threading
from tenacity import retry, stop_after_attempt, wait_exponential
from urllib.parse import urlparse

ROOT_PATH = Path(__file__).resolve().parent.parent.parent
SRC_PATH = ROOT_PATH / "src"
sys.path.append(str(ROOT_PATH))
sys.path.append(str(SRC_PATH))

from utils.http_requests_utils import get_headers
from config.settings import SGX_COMPANY_API_URL, SGX_RESULTS_COUNT_API_URL, COMPANY_LIST_URL, ATTACHMENTS_BASE_URL, RAW_DATA_DIR, PLATFORM, CSS_URL, BACKOFF_FACTOR, REQUEST_TIMEOUT, MAX_RETRIES, PERIOD_END, PERIOD_START, MAX_WORKERS  
import utils.db_utils as db_utils
from bs4 import BeautifulSoup

@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=BACKOFF_FACTOR, min=1, max=REQUEST_TIMEOUT), reraise=True)
def get_search_results(company_name: str = "",
               periodstart: Optional[str] = PERIOD_START,
               periodend: Optional[str] = PERIOD_END,
               exactsearch: bool = False,
               pagestart: int = 0,
               pagesize: int = 20,
               url: str = SGX_COMPANY_API_URL) -> Optional[dict]:
    """Search announcements for a company and return the response as JSON.

    By default uses the previous hardcoded example values. Returns the JSON
    response or None in case of failure.
    """
    params = {
        "periodstart": periodstart,
        "periodend": periodend,
        "value": company_name,
        "exactsearch": str(exactsearch).lower(),
        "pagestart": pagestart,
        "pagesize": pagesize,
    }
    #print(f"Fetching search results for company: {company_name}, pagestart: {pagestart}, pagesize: {pagesize}")
    #print(f"Requesting URL: {url} with params: {params}")
    try:
        documents_response = requests.get(url, params=params, headers=get_headers())
        documents_response.raise_for_status()
        return documents_response.json()
    except RequestException as e:
        print(f"Error fetching search results: {e}")
        from utils.http_requests_utils import fetch_sgx_token
        fetch_sgx_token(force_new=True)  # Refresh token on error
        raise 

def extract_documents_list(response_json: dict) -> Optional[list]:
    """Extracts the list of documents from the search response JSON.

    Returns the list of documents or None if not found.
    """
    try:
        data = response_json.get("data", [])  # Adjusted to match the provided JSON structure
        if isinstance(data, list):
            documents = []
            for item in data:
                documents.append(item)  # Save the entire item instead of specific fields
            return documents
        else:
            print(f"Unexpected data format: {data}")
            return None
    except Exception as e:
        print(f"Error extracting documents list: {e}")
        return None
    
@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=BACKOFF_FACTOR, min=1, max=REQUEST_TIMEOUT), reraise=True)
def request_documents_count(company_name: str,
                        periodstart: Optional[str] = PERIOD_START,
                        periodend: Optional[str] = PERIOD_END,
                        exactsearch: bool = False) -> Optional[int]:
    """Request only the count of announcements for a company.

    Returns the count as an integer, or None on failure.
    """
    params = {
        "periodstart": periodstart,
        "periodend": periodend,
        "value": company_name,
        "exactsearch": str(exactsearch).lower(),
    }
    
    try:
        count_response = requests.get(SGX_RESULTS_COUNT_API_URL, params=params, headers=get_headers())
        count_response.raise_for_status()
        data = count_response.json()

    except requests.exceptions.RequestException as e:
        print(f"Error fetching document count: {e}")
        from utils.http_requests_utils import fetch_sgx_token
        fetch_sgx_token(force_new=True)  # Refresh token on error
        raise

    # Extract the count from the 'data' field
    count = data.get("data")
    if isinstance(count, int):
        return count
    else:
        # If the response format changed, raise to allow retry/examine
        raise ValueError(f"Unexpected response format: {data}")

@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=BACKOFF_FACTOR, min=1, max=REQUEST_TIMEOUT), reraise=True)
def get_web_page(url: str) -> Optional[str]:
    """Fetch a web page and return its HTML content with retries."""
    try:
        response = requests.get(url, headers=get_headers(), timeout=10)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
       print(f"Error fetching web page: {e}")
       from utils.http_requests_utils import fetch_sgx_token
       fetch_sgx_token(force_new=True)  # Refresh token on error
       raise e

def store_web_page(html_content: str, path: str) -> None:
    """Store the fetched web page content into a local file."""
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as file:
            file.write(html_content)
        #print(f"Web page content saved to {path}")
    except Exception as e:
        print(f"Error saving web page content: {e}")

def get_attachments_url_list(html_content: Optional[str]) -> Optional[list]:
    """Fetch attachment URLs from the provided HTML content and return them in the desired format."""
    try:
        if not html_content:
            print("No HTML content provided to extract the attachment URL.")
            raise Exception("HTML content is required")

        # Parse the HTML content
        soup = BeautifulSoup(html_content, "html.parser")
        attachment_list = soup.find("dl", class_="announcement-attachment-list")

        if not attachment_list:
            #print("No attachment list found in the HTML content.")
            return None

        attachment_links = attachment_list.find_all("a", href=True)
        attachments = []

        for link in attachment_links:
            attachment_url = link["href"]
            # Skip links that use JavaScript popups or are not direct file links
            if "JavaScript:window.open" in link.get("onClick", "") or attachment_url.startswith("#") or "#" in attachment_url:
                continue
            # Check if the URL is relative and prepend the base URL if necessary
            if attachment_url.startswith("/"):
                attachment_url = f"{ATTACHMENTS_BASE_URL}{attachment_url}"

            # Format the URL to match the desired format
            formatted_url = attachment_url.replace("/FileOpen/", "").replace("?App=Announcement&FileID=", "_")
            attachments.append(formatted_url)

        if not attachments:
            #print("No attachment links found in the HTML content.")
            return None

        # Check if the HTML content contains the specific string
        if "if you are unable to view the above file, please click the link below" in html_content:
            if len(attachments) == 2:
                attachments.pop(0)
        #print(f"Attachment URLs extracted: {len(attachments)}")
        
        ### DEBUGGING OUTPUT
        #for url in attachments:
        #    print(f"Attachment URL: {url}")
        ###
        
        return attachments
    
    except requests.exceptions.RequestException as req_err:
        print(f"HTTP request error: {req_err}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=BACKOFF_FACTOR, min=1, max=REQUEST_TIMEOUT), reraise=True)
def download_attachment(attachment_url: str, save_path: str) -> None:
    """Download an attachment from a URL and save it to a file with retries."""
    try:
        response = requests.get(attachment_url, headers=get_headers(), timeout=10)
        response.raise_for_status()
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, "wb") as file:
            file.write(response.content)
    except requests.exceptions.RequestException as e:
        print(f"Error downloading attachment: {e}")
        from utils.http_requests_utils import fetch_sgx_token
        fetch_sgx_token(force_new=True)  # Refresh token on error
        raise

def get_document_metadata(document: dict) -> dict:
    """Builds and returns a metadata dictionary for a document."""
    metadata = {
        "document_id": document.get("ref_id", ""),
        "file_type": document.get("sub", ""),
        "category_name": document.get("category_name", ""),
        "title": document.get("title", ""),
        "company_name": document.get("issuer_name", ""),
        "filing_date": document.get("submission_date", ""),
        "url": document.get("url", ""), 
        "platform": PLATFORM,
    }
    return metadata

def store_metadata_debug(metadata: dict, folder_path: str) -> None:
    """Store metadata dictionary as a JSON file for debugging purposes."""
    try:
        # Convert datetime objects to strings
        for key, value in metadata.items():
            if isinstance(value, datetime):
                metadata[key] = value.isoformat()

        os.makedirs(folder_path, exist_ok=True)
        metadata_path = os.path.join(folder_path, "metadata.json")
        with open(metadata_path, "w", encoding="utf-8") as file:
            _json.dump(metadata, file, indent=4)
        #print(f"Metadata saved to {metadata_path}")
    except Exception as e:
        print(f"Error saving metadata: {e}")


    


# Global flag to handle graceful shutdown
shutdown_event = threading.Event()


'''
@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=BACKOFF_FACTOR, min=1, max=REQUEST_TIMEOUT), reraise=True)
def process_document(document: dict, company_id: str, save_raw: bool = True) -> Optional[dict]:
    """Fetch, store and download a single document and its attachments, return metadata.

    Note: do not swallow network/HTTP exceptions here so tenacity can retry on transient failures.
    """
    metadata = get_document_metadata(document)

    # Build folder path for raw storage: RAW_DATA_DIR/SGX/{company_id}_{clean_company}/{category}/{date_ref}/
    issuer = document.get("issuer_name") or document.get("security_name") or metadata.get("company_name", "")
    safe_issuer = "".join(c if c.isalnum() or c in (" ", "_", "-") else "_" for c in issuer).strip().replace(" ", "_")
    category = document.get("cat") or document.get("category_name") or "UNKNOWN"
    date = document.get("submission_date") or ""
    ref = document.get("ref_id") or document.get("id") or ""
    folder_name = f"{company_id}_{safe_issuer}"
    subfolder = os.path.join("SGX", folder_name, category, f"{date}_{ref}")
    full_folder = os.path.join(RAW_DATA_DIR, subfolder)

    # Ensure directory exists
    os.makedirs(full_folder, exist_ok=True)

    # Fetch and store the announcement web page
    url = document.get("url", "")
    if url:
        html = get_web_page(url)
        if html and save_raw:
            store_web_page(html, os.path.join(full_folder, "wp.html"))
    else:
        html = None

    # Extract attachments and download them
    attachments = get_attachments_url_list(html) if html else None
    if attachments:
        attachments_dir = os.path.join(full_folder, "attachments")
        os.makedirs(attachments_dir, exist_ok=True)
        for att in attachments:
            # Skip empty or fragment-only attachment URLs (e.g. '#') which are not downloadable
            if not att or str(att).strip() == "" or str(att).strip().startswith("#"):
                tqdm.write(f"Skipping invalid attachment URL: {att}")
                continue

            att_url = att
            # If relative path, try to prepend base
            if not att_url.startswith("http"):
                att_url = f"{ATTACHMENTS_BASE_URL.rstrip('/')}/{att_url.lstrip('/')}"
            filename = os.path.basename(att_url)
            save_path = os.path.join(attachments_dir, filename)
            # Let download_attachment raise network errors so tenacity can retry the whole document
            download_attachment(att_url, save_path)

    # Store metadata for debugging
    store_metadata_debug(metadata, full_folder)

    return metadata
'''
    

'''
@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=BACKOFF_FACTOR, min=1, max=REQUEST_TIMEOUT), reraise=True)
def process_company_files(company_name: str, company_id: str, pagesize: int = 20, max_workers: int = MAX_WORKERS) -> None:
    """Process all company announcements by paging through search results and processing documents concurrently.

    Do not swallow network/HTTP exceptions at the top level so tenacity can retry on transient failures.
    """
    total = request_documents_count(company_name)
    if total is None:
        # If count retrieval returned None for a non-exception reason, raise to allow retry
        raise RuntimeError(f"Could not retrieve total count for {company_name}")

    pagestart = 0
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while pagestart < total and not shutdown_event.is_set():
            
            results = get_search_results(company_name=company_name, pagestart=pagestart, pagesize=pagesize)
            
            #ERROR IN REQUEST
            if results.json().get('meta')[0].get('code')[0] != 200:
                tqdm.write("Received error response, stopping further processing.")
                raise RuntimeError("Received error response in search results")
            
            #NO DATA FOUND
            if results.json().get('data')[0] is None:
                tqdm.write("No data found for the specified company name")
                raise RuntimeError("No data found for the company ")

            docs = extract_documents_list(results) or []
            if not docs:
                break

            for doc in docs:
                if shutdown_event.is_set():
                    break
                futures.append(executor.submit(process_document, doc, company_id))

        # Wait for completion and surface exceptions
        for fut in tqdm(as_completed(futures), total=len(futures), desc=f"Processing {company_name}"):
            try:
                fut.result()
            except Exception as e:
                tqdm.write(f"Document processing raised: {e}")
'''             

@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=BACKOFF_FACTOR, min=1, max=REQUEST_TIMEOUT), reraise=True)
def download_and_store_css(css_url: str = CSS_URL) -> Optional[str]:
    """Download a CSS file from the given URL and store it in the specified data directory.

    Args:
        css_url (str): The URL of the CSS file to download.

    Returns:
        Optional[str]: The relative path to the stored CSS file, or None if an error occurred.
    """
    try:
        # Fetch the CSS content
        css_content = get_web_page(css_url)
        if not css_content:
            print(f"Failed to fetch CSS content from {css_url}.")
            return None
            # Construct the relative path for the CSS file
        css_filename = css_url.split("/")[-1]
        relative_path = os.path.join("SGX", "_layouts", "1033", "styles", css_filename)
        full_path = os.path.join(RAW_DATA_DIR, relative_path)

        # Ensure the directory exists
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        # Save the CSS content to the file
        with open(full_path, "w", encoding="utf-8") as file:
            file.write(css_content)

        print(f"CSS file saved to {full_path}")
        return relative_path

    except Exception as e:
        print(f"Error downloading or storing CSS file: {e}")
        return None

if __name__ == "__main__":
    company_name_1 = "DBS GROUP HOLDINGS LTD"
    company_name_2 = "ABR HOLDINGS LIMITED"
    #process_company_files(company_name)
    document = {
            "ref_id": "SG250807DVCAPUQT",
            "sub": "CACT06",
            "category_name": "Cash Dividend/ Distribution",
            "submitted_by": "MARC TAN",
            "title": "Cash Dividend/ Distribution::Mandatory",
            "announcer_name": None,
            "issuers": [
                {
                    "isin_code": "SG1L01001701",
                    "stock_code": "D05",
                    "security_name": "DBS GROUP HOLDINGS LTD",
                    "issuer_name": "DBS GROUP HOLDINGS LTD",
                    "ibm_code": "1L01"
                }
            ],
            "security_name": "DBS GROUP HOLDINGS LTD",
            "url": "https://links.sgx.com/1.0.0/corporate-announcements/HJ9N9C94JRSF1Q1Z/5c51c5cbd54535b0220380856bc5cdd41cf2fc172c0781bcedbbbbef186f9cba",
            "issuer_name": "DBS GROUP HOLDINGS LTD",
            "submission_date": "20250807",
            "submission_date_time": 1754519573000,
            "broadcast_date_time": 1754519573000,
            "xml": None,
            "submission_time": None,
            "cat": "CACT",
            "id": "HJ9N9C94JRSF1Q1Z",
            "sn": None,
            "product_category": None
        }

    '''
    with open("B:\\Workspace\\sgx-pipeline\\raw_data_storage\\SGX\\ID56789_ABR_HOLDINGS_LIMITED\\ANNC14\\20250321_SG250321OTHR1194\\wp.html", "r", encoding="utf-8") as file:
        html = file.read()
    '''
    #att_list = get_attachments_url_list(html)
    #print(f"Attachment list: {att_list}")
    '''
    metadata = process_document(document, "ID12345")
    if metadata:
        print("Document processed successfully.")
        print(metadata)
        
    else:
        print("Document processing failed.")
    '''

    #process_company_files(company_name_2, "ID56789")
    import json
    from config.settings import PROJECT_ROOT 
    listed_company = json.load(open(PROJECT_ROOT + "/data/listed_company_list.json")).get("companyName", [])
    num_companies = len(listed_company)
    print(f"Total listed companies: {num_companies}")

    for company in listed_company:
        results = get_search_results(company_name=company, exactsearch=False)
        #results = get_search_results(company_name="DBS BANK LTD.")

        # Defensive extraction: `results.get("data")` is typically a list of document dicts.
        data = results.get("data") if isinstance(results, dict) else []
        issuer_names = []
        for item in data or []:
            if not isinstance(item, dict):
                continue
            issuer = item.get("issuer_name") or item.get("security_name")
            if not issuer:
                issuers = item.get("issuers")
                if isinstance(issuers, (list, tuple)) and len(issuers) > 0 and isinstance(issuers[0], dict):
                    issuer = issuers[0].get("issuer_name") or issuers[0].get("security_name")
            if issuer:
                issuer_names.append(issuer.strip())

        unique_issuers = set(issuer_names)
        print(unique_issuers)

        #doc_list = extract_documents_list(results)
        #print (f"Document list: {doc_list}")


        #download_and_store_css("https://links.sgx.com/_layouts/1033/styles/infoviewstyle.css")
