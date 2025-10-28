"""Safer scraper utilities for SGX announcements.

This module provides small helpers to call the SGX announcements API
without embedding sensitive values directly into URLs or module-level
constants that may not be defined in the runtime environment.

"""

from typing import Optional
import requests
import json as _json
import os   
# Add the src directory to the Python path to allow imports from the entire src folder
from pathlib import Path
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import signal
import threading
from tenacity import retry, stop_after_attempt, wait_exponential

ROOT_PATH = Path(__file__).resolve().parent.parent.parent
SRC_PATH = ROOT_PATH / "src"
sys.path.append(str(ROOT_PATH))
sys.path.append(str(SRC_PATH))

from utils.http_requests_utils import get_headers
from config.settings import SGX_COMPANY_API_URL, SGX_RESULTS_COUNT_API_URL, PROJECT_ROOT, ATTACHMENTS_BASE_URL, RAW_DATA_DIR, PLATFORM, PAGE_SIZE, MAX_PAGES, MAX_WORKERS, MAX_FILES_PER_COMPANY, CSS_URL, BACKOFF_FACTOR, REQUEST_TIMEOUT, MAX_RETRIES
import utils.db_utils as db_utils
from utils import document_worker
from bs4 import BeautifulSoup

@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=BACKOFF_FACTOR, min=1, max=REQUEST_TIMEOUT))
def get_search_results(company_name: str = "DBS GROUP HOLDINGS LTD",
               periodstart: Optional[str] = "20250104_160000",
               periodend: Optional[str] = "20251021_120000",
               exactsearch: bool = True,
               pagestart: int = 0,
               pagesize: int = 20) -> Optional[dict]:
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

    try:
        
        documents_response = requests.get(SGX_COMPANY_API_URL, params=params, headers=get_headers())
        documents_response.raise_for_status()
        return documents_response.json()
    except requests.exceptions.RequestException as req_err:
        print(f"HTTP request error: {req_err}")
        return None
    except ValueError as json_err:
        print(f"JSON decoding error: {json_err}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None
    
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
    
@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=BACKOFF_FACTOR, min=1, max=REQUEST_TIMEOUT))
def request_documents_count(company_name: str,
                        periodstart: Optional[str] = "20250204_160000",
                        periodend: Optional[str] = "20251022_120000",
                        exactsearch: bool = True) -> Optional[int]:
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

        # Extract the count from the 'data' field
        count = data.get("data")
        if isinstance(count, int):
            return count
        else:
            print(f"Unexpected response format: {data}")
            return None
    except requests.exceptions.RequestException as req_err:
        print(f"HTTP request error: {req_err}")
        return None
    except ValueError as json_err:
        print(f"JSON decoding error: {json_err}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=BACKOFF_FACTOR, min=1, max=REQUEST_TIMEOUT))
def get_web_page(url: str) -> Optional[str]:
    """Fetch a web page and return its HTML content with retries."""
    response = requests.get(url, headers=get_headers(), timeout=10)
    response.raise_for_status()
    return response.text



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

@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=BACKOFF_FACTOR, min=1, max=REQUEST_TIMEOUT))
def download_attachment(attachment_url: str, save_path: str) -> None:
    """Download an attachment from a URL and save it to a file with retries."""
    response = requests.get(attachment_url, headers=get_headers(), timeout=10)
    response.raise_for_status()
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    with open(save_path, "wb") as file:
        file.write(response.content)

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

def signal_handler(sig, frame):
    """Signal handler to set the shutdown event."""
    print("\nGraceful shutdown initiated. Waiting for threads to complete...")
    shutdown_event.set()

# Register the signal handler for SIGINT (Ctrl+C)
signal.signal(signal.SIGINT, signal_handler)

def process_company(company_name: str, company_id: str):
    """Process all documents for a given company."""
    n_results = request_documents_count(company_name=company_name)
    if n_results is None:
        print(f"Failed to retrieve document count for {company_name}")
        return

    n_pages = (n_results + PAGE_SIZE - 1) // PAGE_SIZE  # Calculate number of pages needed
    all_documents = []

    # Collect all documents from all pages
    for page_num in range(min(n_pages, MAX_PAGES) if MAX_PAGES > 0 else n_pages):
        if shutdown_event.is_set():
            print("Shutdown event detected. Exiting document collection loop.")
            return

        print(f"Processing page {page_num + 1}/{n_pages} (page size {PAGE_SIZE}) for {company_name}")
        response = get_search_results(company_name=company_name, pagesize=PAGE_SIZE, pagestart=page_num)

        if not response:
            print(f"Failed to retrieve search results for {company_name}")
            continue

        doc_list = extract_documents_list(response)
        if doc_list:
            all_documents.extend(doc_list)
            if (len(all_documents) >= MAX_FILES_PER_COMPANY) and (MAX_FILES_PER_COMPANY > 0):
                print(f"Reached maximum documents per company limit: {MAX_FILES_PER_COMPANY}")
                all_documents = all_documents[:MAX_FILES_PER_COMPANY]
                break

    print(f"Total documents collected: {len(all_documents)}")
    # Extract and print the list of document IDs for debugging
    document_ids = [doc.get("ref_id", "Unknown ID") for doc in all_documents]
    print(f"Document IDs: {document_ids}")

    all_metadata = []
    
    # Process all documents using multithreading
    if all_documents:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for doc in all_documents:
                futures.append(executor.submit(document_worker.process_document, doc, company_id))

            with tqdm(total=len(all_documents), desc="Processing documents") as pbar:
                for future in as_completed(futures):
                    if shutdown_event.is_set():
                        print("Shutdown event detected. Cancelling remaining tasks.")
                        executor.shutdown(wait=False, cancel_futures=True)
                        return

                    try:
                        doc_metadata = future.result()
                        if doc_metadata:
                            all_metadata.append(doc_metadata)
                    except Exception as e:
                        print(f"Error processing document: {e}")
                    finally:
                        pbar.update(1)
                        
        # Store all metadata in the database
        from utils.db_utils import store_metadata_batch

        try:
            store_metadata_batch(all_metadata)
        except Exception as e:
            print(f"Error storing metadata: {e}")

    # Further processing can be done here


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
    #process_company(company_name)
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

    #process_company(company_name_2, "ID56789")

    '''
    results = get_search_results(company_name_2, "ID56789")
    print(f"Search results: {results}")
    doc_list = extract_documents_list(results)
    print (f"Document list: {doc_list}")
    '''

    download_and_store_css("https://links.sgx.com/_layouts/1033/styles/infoviewstyle.css")
