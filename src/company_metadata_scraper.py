def run_company_metadata_pipeline():
    from utils.scraping_utils import download_and_store_css
    try:
        download_and_store_css()
    except Exception as e:
        print(f"Error downloading and storing CSS: {e}")
        # continue anyway

    from populate_collections import populate_company_collections
    try:
        populate_company_collections()
    except Exception as e:
        print(f"Error populating company collections: {e}")
        return

    from utils.db_utils import get_companies_without_metadata, update_company_metadata
    pending_companies = get_companies_without_metadata()

    if not pending_companies:
        print("No pending companies to process for metadata.")
        return

    # Progress and statistics (multithreaded)
    from tqdm import tqdm
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from config.settings import MAX_WORKERS

    processed_ok = 0
    processed_error = 0
    error_companies = []

    def _process_company(company):
        company_name = company.get("name")
        company_id = company.get("company_id")
        if not company_name or not company_id:
            return (company_id, company_name, False, "invalid data")
        try:
            metadata = get_company_metadata(company_name, company_id)
            if not metadata:
                return (company_id, company_name, False, "empty metadata")
            # Persist metadata (thread-safe: update_company_metadata creates its own DB connection)
            update_company_metadata(company_id, metadata=metadata)
            return (company_id, company_name, True, "")
        except Exception as e:
            return (company_id, company_name, False, str(e))

    workers = MAX_WORKERS if isinstance(MAX_WORKERS, int) and MAX_WORKERS > 0 else 4
    futures = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for company in pending_companies:
            futures.append(executor.submit(_process_company, company))

        for fut in tqdm(as_completed(futures), total=len(futures), desc="Companies (metadata)"):
            cid, cname, ok, msg = fut.result()
            if ok:
                processed_ok += 1
            else:
                processed_error += 1
                error_companies.append((cid, cname, msg))

    # Summary
    print("\nMetadata processing summary:")
    print(f"  Total companies considered: {len(pending_companies)}")
    print(f"  Successfully processed:    {processed_ok}")
    print(f"  Errors:                    {processed_error}")
    if error_companies:
        print("  Error details (company_id, name, error):")
        for cid, cname, cerr in error_companies:
            print(f"    - {cid} | {cname} | {cerr}")

def get_company_metadata(company_name=None, company_id=None) -> dict:
    from utils.scraping_utils import get_web_page
    
    from config.settings import COMPANY_PAGE_URL
    import os
    url = os.path.join(COMPANY_PAGE_URL, str(company_id))
    html_content = get_web_page(url)
    if (not html_content) or (len(html_content.strip()) == 0):
        raise ValueError(f"Failed to retrieve corporate information page for company ID {company_id}")
    # Implements the logic to extract metadata from html_content
    try:
        metadata = parse_company_metadata(html_content)
        if ( not metadata) or (len(metadata) == 0):
            raise ValueError(f"No metadata extracted for company ID {company_id}")
        return metadata
    except Exception as e:
        raise e

def parse_company_metadata(html_content: str) -> dict:
    """
    Extract the required fields from an SGX company page in a simple, readable way.
    Extracted fields:
      - full_company_name, incorporated_in, incorporated_on, isin_code,
        registered_office (list), telephone, fax, email, secretary, website
    """
    from bs4 import BeautifulSoup
    from datetime import datetime
    import re

    soup = BeautifulSoup(html_content or "", "html.parser")

    if (not soup) or (len(soup.find_all()) == 0):
        raise ValueError("Empty or invalid HTML content")
    
    def text_by_id(elem_id: str):
        el = soup.find(id=elem_id)
        return el.get_text(strip=True) if el else None

    def dd_list_after_label(label_text: str):
        # Find a <dt> whose text contains label_text (case-insensitive),
        # then return the stripped strings from the following <dd> as a list.
        dt = soup.find(lambda tag: tag.name == "dt" and label_text.lower() in tag.get_text().lower())
        if not dt:
            return None
        dd = dt.find_next_sibling("dd")
        if not dd:
            return None
        items = [s.strip() for s in dd.stripped_strings if s.strip()]
        return items if items else None

    def join_or_none(lst):
        return " ".join(lst) if lst else None

    # Prefer explicit element IDs, otherwise fall back to dt/dd pairs
    full_name = (
        text_by_id("ctl07_compFullNameLabel")
        or text_by_id("ctl07_lblCompName")
        or text_by_id("ctl07_lblIPOCompanyName")
    )
    incorporated_in = text_by_id("ctl07_incorporatedLabel") or join_or_none(dd_list_after_label("Incorporated in"))
    incorporated_on = text_by_id("ctl07_incorpOnLabel") or join_or_none(dd_list_after_label("Incorporated on"))
    isin_code = text_by_id("ctl07_isinCodeLabel") or join_or_none(dd_list_after_label("ISIN"))

    # Registered office: try line-by-line IDs first, otherwise use dt/dd fallback
    reg_lines = []
    for i in range(1, 5):
        v = text_by_id(f"ctl07_regOffc{i}Label")
        if v:
            reg_lines.append(v)
    if not reg_lines:
        reg_lines = dd_list_after_label("Registered Office") or []

    telephone = text_by_id("ctl07_teleLabel") or join_or_none(dd_list_after_label("Telephone"))
    fax = text_by_id("ctl07_faxLabel") or join_or_none(dd_list_after_label("Fax"))
    email = text_by_id("ctl07_emailLabel") or join_or_none(dd_list_after_label("Email"))

    # Secretary: combine explicit IDs if present, otherwise use dt/dd
    sec_parts = [
        p for p in (text_by_id("ctl07_secretary1Label"), text_by_id("ctl07_secretary2Label")) if p
    ]
    secretary = " ".join(sec_parts) if sec_parts else join_or_none(dd_list_after_label("Secretary"))

    # Website: prefer explicit link ID, otherwise take the first absolute link found
    link = soup.find("a", id="ctl07_compWebHypLink")
    
    #Date of last update
    # Website: prefer explicit link ID, otherwise take the first absolute link found
    website = None
    if link and link.get("href"):
        website = link.get("href").strip()
    else:
        first_abs = soup.find("a", href=re.compile(r"^https?://", re.I))
        if first_abs and first_abs.get("href"):
            website = first_abs.get("href").strip()

    # Date of last update: prefer explicit ID, otherwise try to locate the text near the label
    last_update_date = None
    last_update_text = text_by_id("ctl07_lblLastUpdatedOn") or text_by_id("ctl07_lblModifyOn")

    if not last_update_text:
        # Look for the label text and then a nearby span containing the date
        label_node = soup.find(string=re.compile(r"Information last updated on", re.I))
        if label_node:
            parent = label_node.find_parent()
            if parent:
                span = parent.find("span", id=re.compile(r"ctl07_lblLastUpdatedOn", re.I))
                if span:
                    last_update_text = span.get_text(strip=True)

    if last_update_text:
        # Try common SGX format "MM/DD/YYYY hh:mm:ss AM/PM"
        for fmt in ("%m/%d/%Y %I:%M:%S %p", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
            try:
                dt = datetime.strptime(last_update_text, fmt)
                last_update_date = dt.isoformat()
                break
            except Exception:
                continue
        if last_update_date is None:
            # fallback: keep raw text if it cannot be parsed
            last_update_date = last_update_text.strip()
    
    return {
        "full_company_name": full_name,
        "incorporated_in": incorporated_in,
        "incorporated_on": incorporated_on,
        "isin_code": isin_code,
        "registered_office": reg_lines or None,
        "telephone": telephone,
        "fax": fax,
        "email": email,
        "secretary": secretary,
        "website": website,
        "date_of_last_update": last_update_date
    }
   
    
if __name__ == "__main__":
    run_company_metadata_pipeline()
    #metadata = get_company_metadata(company_id=2995)
    #print(metadata)