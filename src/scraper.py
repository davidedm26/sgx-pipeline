from utils.scraper_utils import run_search, request_documents_count, get_attachments_url_list, download_attachment
from config.settings import PROJECT_ROOT

def process_company(company_name: str):
    response = run_search(company_name=company_name)
    
    if response:
        print(f"Search results for {company_name}")
    else:
        print(f"Failed to retrieve search results for {company_name}")

    count = request_documents_count(company_name=company_name)
    if count is not None:
        print(f"Document count for {company_name}: {count}")
    else:
        print(f"Failed to retrieve document count for {company_name}")

    # Further processing can be done here

if __name__ == "__main__":
    company_name = "DBS GROUP HOLDINGS LTD"
    process_company(company_name)