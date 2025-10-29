
def run_pipeline():
    from populate_queue import populate_company_queue
    try:
        populate_company_queue()
    except Exception as e:
        print(f"Error populating company queue: {e}")
        return
    
    from utils.db_utils import get_pending_companies
    pending_companies = get_pending_companies()
    
    for company in pending_companies:
        company_name = company.get("name")
        company_id = company.get("company_id")
        if not company_name or not company_id:
            print(f"Invalid company data: {company}")
            continue
        
        
        try:
            from company_document_scraper import process_company
            #update status to running
            process_company(company_name, company_id)
            
            #update status to 'processed' in the database
        except Exception as e:
            print(f"Error processing documents for company {company_name}: {e}")
            
            #update status to 'error' in the database
            
            # Update company status to 'processed' in the database
            #from utils.db_utils import update_company_status
            #update_company_status(company_id, status="processed")
            pass
            
        except Exception as e:
            print(f"Error processing company {company_name}: {e}")
        
if __name__ == "__main__":
    run_pipeline()