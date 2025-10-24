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
import time
from datetime import datetime, timezone
ROOT_PATH = Path(__file__).resolve().parent.parent.parent
SRC_PATH = ROOT_PATH / "src"
sys.path.append(str(ROOT_PATH))
sys.path.append(str(SRC_PATH))

from utils.http_requests_utils import get_headers
from config.settings import SGX_COMPANY_API_URL, SGX_RESULTS_COUNT_API_URL, PROJECT_ROOT, ATTACHMENTS_BASE_URL, RAW_DATA_DIR, PLATFORM 
from bs4 import BeautifulSoup

def get_search_results(company_name: str = "DBS GROUP HOLDINGS LTD",
               periodstart: Optional[str] = "20250204_160000",
               periodend: Optional[str] = "20251022_120000",
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

def get_web_page(url: str) -> Optional[str]:
    """Fetch a web page and return its HTML content."""
    try:
        response = requests.get(url, headers=get_headers())
        response.raise_for_status()
        
        # Save the page content
        #with open(debug_path, "w", encoding="utf-8") as file:
        #    file.write(response.text)
        #print(f"Web page content saved to {debug_path}")

        return response.text
    except requests.exceptions.RequestException as req_err:
        print(f"HTTP request error: {req_err}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None
    
def store_web_page(html_content: str, path: str) -> None:
    """Store the fetched web page content into a local file."""
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as file:
            file.write(html_content)
        print(f"Web page content saved to {path}")
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

def download_attachment(attachment_url: str, save_path: str) -> None:
    """Download an attachment from a URL and save it to a file."""
    try:
        response = requests.get(attachment_url, headers=get_headers())
        response.raise_for_status()

        try:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            with open(save_path, "wb") as file:
                file.write(response.content)
        except IOError as io_err:
            print(f"File I/O error: {io_err}")
        except Exception as e:
            print(f"Unexpected error while saving the file: {e}")
        
        print(f"Attachment downloaded and saved to {save_path}")
    except requests.exceptions.RequestException as req_err:
        print(f"HTTP request error: {req_err}")
    except Exception as e:
        print(f"Unexpected error: {e}")

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
        print(f"Metadata saved to {metadata_path}")
    except Exception as e:
        print(f"Error saving metadata: {e}")

def process_document(document: dict, company_id: str) -> bool:
    """Process a single document: fetch its web page, store it, and download attachments."""
    try:
        # Build document metadata dictionary
        metadata = get_document_metadata(document)
        metadata["company_id"] = company_id
        print(f"Metadata for document {document.get('id')}: {metadata}")
        url = metadata.get("url")
        if not url:
            raise ValueError("Document URL is missing")
        
        wp = get_web_page(url)
        
        company_name_no_space = "_".join(metadata.get("company_name").replace(" ", "_").split())
        filing_date_str = metadata.get("filing_date")
        document_id = metadata.get('document_id')
        file_type = metadata.get('file_type')

        document_folder = os.path.join(RAW_DATA_DIR, PLATFORM, f"{company_id}_{company_name_no_space}", f"{file_type}" , f"{filing_date_str}_{document_id}")

        #Transform filing_date in timestamp format
        
        if filing_date_str:
            try:
                filing_date = datetime.strptime(filing_date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
                print(f"Transformed filing_date {filing_date_str} to {filing_date}")
                metadata["filing_date"] = filing_date
                
            except ValueError as e:
                print(f"Error parsing filing_date: {e}")

        wp_filename = "wp.html"
        wp_path = os.path.join(document_folder, wp_filename)
        
        relative_path = os.path.relpath(wp_path, RAW_DATA_DIR)
        metadata["file_path"] = relative_path
        
        store_web_page(wp,  wp_path)
        
        #print(f"Metadata for document {document.get('id')}: {metadata}")

        att_list = get_attachments_url_list(wp)
        #print(f"Attachment list: {att_list}")
        if (att_list and len(att_list) > 0):
            for idx, att in enumerate(att_list or []):
                att_filename = f"attachment_{document.get('id')}_{idx}.pdf"
                att_path = os.path.join(document_folder, att_filename)
                download_attachment(att, att_path)
                
                relative_att_path = os.path.relpath(att_path, RAW_DATA_DIR)
                # Store relative path in metadata
                if "supporting_file_paths" not in metadata:
                    metadata["supporting_file_paths"] = []
                metadata["supporting_file_paths"].append(relative_att_path)
        else:
            print("No attachments found for this document.")
        
        store_metadata_debug(metadata, document_folder) #need to be replaced with the db storage function
    except Exception as e:
        print(f"Error processing document: {e}")
        return False
    return True

def process_company(company_name: str, company_id: str):
    """Process all documents for a given company."""
    #The company_id is passed because it is needed in metadata storage
    
    #inserisci paginazione, ricava nr pagine dalla richiesta count e poi itera per ciascuna pagina
    #inserisci tutti i metadati ottenuti in metadata_batch[]
    
    response = get_search_results(company_name=company_name, pagesize=20)
    
    if response:
        print(f"Search results for {company_name}")
    else:
        print(f"Failed to retrieve search results for {company_name}")

    count = request_documents_count(company_name=company_name)
    if count is not None:
        print(f"Document count for {company_name}: {count}")
    else:
        print(f"Failed to retrieve document count for {company_name}")
    
    doc_list = extract_documents_list(response)
    print(f"Extracted {len(doc_list) if doc_list else 0} documents.")
    
    for doc in doc_list or []:
        #Qui introduci Multithreading
        try:
            process_document(doc, company_id)
        except Exception as e:
            print(f"Error processing document: {e}")   
    

    # Further processing can be done here
if __name__ == "__main__":
    company_name = "DBS GROUP HOLDINGS LTD"
    #process_company(company_name)
    document = {
            "ref_id": "SG251013INTRBN3C",
            "sub": "CACT25",
            "category_name": "Coupon Payment",
            "submitted_by": "DBS BANK LTD",
            "title": "Coupon Payment::Mandatory",
            "announcer_name": None,
            "issuers": [
                {
                    "isin_code": "AU3FN0056685",
                    "stock_code": "XUTB",
                    "security_name": "DBS GRP AUD300M F310408",
                    "issuer_name": "DBS GROUP HOLDINGS LTD",
                    "ibm_code": "45MD"
                }
            ],
            "security_name": "DBS GRP AUD300M F310408",
            "url": "https://links.sgx.com/1.0.0/corporate-announcements/R4UE4P9H1B27AUR1/c5a7ef342cbb05fc5ac7043d8fe900eef61c9ea5d2c90df67ccb364c06e9a7fb",
            "issuer_name": "DBS GROUP HOLDINGS LTD",
            "submission_date": "20251013",
            "submission_date_time": 1760337125000,
            "broadcast_date_time": 1760337126000,
            "xml": None,
            "submission_time": None ,
            "cat": "CACT",
            "id": "R4UE4P9H1B27AUR1",
            "sn": None,
            "product_category": None
        }
    

    if process_document(document, "ID12345"):
        print("Document processed successfully.")
    else:
        print("Document processing failed.")
