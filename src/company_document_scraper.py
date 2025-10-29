from config.settings import PAGE_SIZE, MAX_PAGES, MAX_WORKERS, MAX_FILES_PER_COMPANY, BATCH_SIZE
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from utils.db_utils import store_metadata_batch


def process_company(company_name: str, company_id: str):
    """Process all documents for a given company."""
    # Import `document_worker` locally to avoid circular dependencies
    from utils import document_worker
    from utils.scraping_utils import (
        request_documents_count,
        get_search_results,
        extract_documents_list,
    )
    n_results = request_documents_count(company_name=company_name)
    tqdm.write(f"Number of documents found for {company_name}: {n_results}")
    if n_results is None:
        tqdm.write(f"Failed to retrieve document count for {company_name}")
        return

    n_pages = (n_results + PAGE_SIZE - 1) // PAGE_SIZE  # Calculate number of pages needed
    all_documents = []

    # Collect all documents from all pages
    for page_num in range(min(n_pages, MAX_PAGES) if MAX_PAGES > 0 else n_pages):
        from utils.scraping_utils import shutdown_event
        if shutdown_event.is_set():
            tqdm.write("Shutdown event detected. Exiting document collection loop.")
            return

        tqdm.write(f"Processing page {page_num + 1}/{n_pages} (page size {PAGE_SIZE}) for {company_name}")
        response = get_search_results(company_name=company_name, pagesize=PAGE_SIZE, pagestart=page_num)

        if not response:
            tqdm.write(f"Failed to retrieve search results for {company_name}")
            continue

        doc_list = extract_documents_list(response)
        if doc_list:
            all_documents.extend(doc_list)
            if (len(all_documents) >= MAX_FILES_PER_COMPANY) and (MAX_FILES_PER_COMPANY > 0):
                tqdm.write(f"Reached maximum documents per company limit: {MAX_FILES_PER_COMPANY}")
                all_documents = all_documents[:MAX_FILES_PER_COMPANY]
                break

    tqdm.write(f"Total documents collected: {len(all_documents)}")
    # Extract and print the list of document IDs for debugging
    document_ids = [doc.get("ref_id", "Unknown ID") for doc in all_documents]
    tqdm.write(f"Document IDs: {document_ids}")

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
                        tqdm.write("Shutdown event detected. Cancelling remaining tasks.")
                        executor.shutdown(wait=False, cancel_futures=True)
                        return

                    try:
                        doc_metadata = future.result()
                        if doc_metadata:
                            all_metadata.append(doc_metadata)
                            if len(all_metadata) >= BATCH_SIZE:  # Adjust batch size as needed
                                try:
                                    batch_to_store = all_metadata[:BATCH_SIZE]  # Copy the first BATCH_SIZE elements
                                    store_metadata_batch(batch_to_store)  # Store metadata in batches
                                    del all_metadata[:BATCH_SIZE]  # Remove the first BATCH_SIZE elements safely
                                    tqdm.write(f"Saved {len(batch_to_store)} new documents in sgx-public-documents-granular.")
                                except Exception as e:
                                    tqdm.write(f"Error storing metadata batch: {e}")
                    except Exception as e:
                        tqdm.write(f"Error processing document: {e}")
                    finally:
                        pbar.update(1)

        # Store all metadata in the database
        try:
            store_metadata_batch(all_metadata)  # Store all metadata at once in the database
            tqdm.write(f"Saved {len(all_metadata)} new documents in sgx-public-documents-granular.")
        except Exception as e:
            tqdm.write(f"Error storing metadata: {e}")
            
if __name__ == "__main__":
    # Example usage
    company_name = "BONVESTS HOLDINGS LIMITED"
    company_id = "981"
    process_company(company_name, company_id)