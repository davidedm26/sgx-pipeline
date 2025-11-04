# /b:/Workspace/sgx-pipeline/src/pipeline.py

current_company = {"name": None, "id": None}

def run_pipeline():
    from utils.scraping_utils import download_and_store_css
    try:
        download_and_store_css()
    except Exception as e:
        print(f"Error downloading and storing CSS: {e}")
        #continue anyway
    
    from populate_collections import populate_company_collections
    try:
        populate_company_collections()
    except Exception as e:
        print(f"Error populating company collections: {e}")
        return
    
    from utils.db_utils import reset_error_companies 
    try:
        reset_error_companies()
    except Exception as e:
        print(f"Error resetting error companies: {e}")
        return    
    
    from utils.db_utils import get_pending_companies
    pending_companies = get_pending_companies()
    
    if (not pending_companies) or (len(pending_companies) == 0):
        print("No pending companies to process.")
        return
    
    for company in pending_companies:
        company_name = company.get("name")
        company_id = company.get("company_id")
        if not company_name or not company_id:
            print(f"Invalid company data: {company}")
            continue # skip invalid entries

        current_company["name"] = company_name
        current_company["id"] = company_id

        try:
            try:
                process_company(company_name, company_id)
            except KeyboardInterrupt:
                print("Interruption received (Ctrl+C). Marking current company as 'cancelled' and exiting.")
                try:
                    from utils.db_utils import update_company
                    update_company(company_id, status="cancelled", processed=False)
                except Exception as e:
                    print(f"Failed to update cancelled status for company {company_name}: {e}")
                raise  # Rilancia l'eccezione per uscire dal ciclo principale
            except Exception as e:
                print(f"Error processing company {company_name}: {e}")
                update_company(company_id, status="error", processed=False)
        finally:
            current_company["name"] = None
            current_company["id"] = None
        
def process_company(company_name, company_id):
    from utils.db_utils import update_company
    from company_document_scraper import process_company_files
    # update status to 'running' in the database
    try:
        update_company(company_id, status="running")
    except Exception as e:
        print(f"Error updating running status for company {company_name}: {e}")
        return
    try:
        process_company_files(company_name, company_id)
    except Exception as e:
        print(f"Error processing files for company {company_name}: {e}")
        update_company(company_id, status="error", processed=False)
        return
    try:
        # update status to 'success' in the database
        update_company(company_id, status="success", processed=True)
    except Exception as e:
        print(f"Error updating success status for company {company_name}: {e}")


if __name__ == "__main__":
    try:
        run_pipeline()
    except KeyboardInterrupt:
        # Catch any KeyboardInterrupt that escaped and report it
        name = current_company.get("name")
        cid = current_company.get("id")
        if name and cid:
            try:
                from utils.db_utils import update_company
                update_company(cid, status="cancelled", processed=False)
            except Exception as e:
                print(f"Failed to update cancelled status for company {name}: {e}")
        print("Interrupted by the user (Ctrl+C).")