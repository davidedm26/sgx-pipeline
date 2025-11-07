from config.settings import FILES_PAGE_SIZE, FILES_MAX_PAGES, MAX_WORKERS, MAX_FILES_PER_COMPANY, BATCH_SIZE
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from tenacity import RetryError
import requests
from utils.db_utils import store_metadata_batch


def process_company_files(company_name: str, company_id: str):
    """Process all documents for a given company."""
    # Import `document_worker` locally to avoid circular dependencies
    from utils import document_worker
    from utils.scraping_utils import (
        request_documents_count,
        get_search_results,
        extract_documents_list,
    )
    
    n_results = request_documents_count(company_name=company_name)
    
    if n_results is None:
        tqdm.write(f"Failed to retrieve document count for {company_name}")
        raise ValueError("Could not get document count")
    
    tqdm.write(f"Number of documents found for {company_name}: {n_results}")
    
    n_pages = (n_results + FILES_PAGE_SIZE - 1) // FILES_PAGE_SIZE  # Calculate number of pages needed
    all_documents = []

    # Collect all documents from all pages
    for page_num in range(min(n_pages, FILES_MAX_PAGES) if FILES_MAX_PAGES > 0 else n_pages):
        from utils.scraping_utils import shutdown_event
        if shutdown_event.is_set():
            tqdm.write("Shutdown event detected. Exiting document collection loop.")
            return

        #tqdm.write(f"Processing page {page_num + 1}/{n_pages} (page size {FILES_PAGE_SIZE}) for {company_name}")
        
        try:
            response = get_search_results(company_name=company_name, pagesize=FILES_PAGE_SIZE, pagestart=page_num)
        except Exception as e:
            tqdm.write(f"Error retrieving search results for {company_name} on page {page_num + 1}: {e}")
            raise e
            #break

        if not response:
            tqdm.write(f"Failed to retrieve search results for {company_name}")
            raise ValueError("No response received")

        # if response.get('data') is None:
        #    raise ValueError("No data found in response")
        if response.get('meta', {}).get('code') != "200":
            raise ValueError("Error code in response")

        doc_list = extract_documents_list(response)

        
        if doc_list:
            all_documents.extend(doc_list)
            if (len(all_documents) >= MAX_FILES_PER_COMPANY) and (MAX_FILES_PER_COMPANY > 0):
                tqdm.write(f"Reached maximum documents per company limit: {MAX_FILES_PER_COMPANY}")
                all_documents = all_documents[:MAX_FILES_PER_COMPANY]
                break

    tqdm.write(f"Total documents collected for {company_name}: {len(all_documents)}")
    # Extract and print the list of document IDs for debugging
    document_ids = [doc.get("ref_id", "Unknown ID") for doc in all_documents]
    #tqdm.write(f"Document IDs: {document_ids}")

    all_metadata = []
    
    # Process all documents using multithreading
    if all_documents:

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(document_worker.process_document, doc, company_id) for doc in all_documents]

            # Use tqdm and update the bar each time a future completes
            with tqdm(total=len(futures), desc=f"Processing documents for {company_name}") as pbar:
                for future in as_completed(futures):
                    if shutdown_event.is_set():
                        tqdm.write("Shutdown event detected. Cancelling remaining tasks.")
                        try:
                            executor.shutdown(wait=False, cancel_futures=True)
                        except TypeError:
                            # cancel_futures is not available on older Python versions
                            executor.shutdown(wait=False)
                        break

                    try:
                        doc_metadata = future.result()
                        if doc_metadata:
                            all_metadata.append(doc_metadata)
                            '''
                            # Store in batches if BATCH_SIZE is configured (> 0)
                            if BATCH_SIZE > 0:
                                while len(all_metadata) >= BATCH_SIZE:
                                    batch_to_store = all_metadata[:BATCH_SIZE]
                                    try:
                                        store_metadata_batch(batch_to_store)
                                        del all_metadata[:len(batch_to_store)]
                                        tqdm.write(f"Saved {len(batch_to_store)} new documents in sgx-public-documents-granular.")
                                    except Exception as e:
                                        tqdm.write(f"Error storing metadata batch: {e}")
                                        # stop attempting to store further batches on persistent error
                                        break
                            '''
                    except RetryError as re:
                        tqdm.write(f"Request failed after retries: {re}")
                    except requests.exceptions.RequestException as rexc:
                        tqdm.write(f"Network error during document processing: {rexc}")
                    except Exception as e:
                        tqdm.write(f"Unexpected error processing document: {e}")
                    finally:
                        # Important: update the progress bar for every completed future
                        pbar.update(1)

        # Store metadata in the database (if any)
        if all_metadata:
            try:
                tqdm.write(f"Storing {len(all_metadata)} documents in sgx-public-documents-granular.")
                store_metadata_batch(all_metadata)
                #tqdm.write(f"Saved {len(all_metadata)} new documents in sgx-public-documents-granular.")
                all_metadata.clear()
            except Exception as e:
                tqdm.write(f"Error storing metadata: {e}")
        else:
            tqdm.write("No new metadata to store.")
            
if __name__ == "__main__":
    # Example usage
    company_name = "SINOSTAR PEC HOLDINGS LIMITED"
    company_id = "2788"
    process_company_files(company_name, company_id)
    