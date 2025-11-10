# This script scrape the ticker list from the SGX website (ticker - ticker_name) and then match the ticker name with the official company name the database and then update metadata (uat/prod) accordingly

import requests
import os
import json
from utils.http_requests_utils import get_headers

def scrape_sgx_ticker_list():
    url = "https://api.sgx.com/securities/v1.1"
    headers = get_headers()
    response = requests.get(url, headers=headers, timeout=10)
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve data from SGX. Status code: {response.status_code}")

    data = response.json()
    
    ticker_list = []
    for item in data.get("data", []).get("prices", []):
        if (item.get("type") in  ["stocks", "reits", "businesstrusts", "etfs"]) is False:
            continue
        ticker = item.get("nc")
        name = item.get("n")
        if ticker and name:
            ticker_list.append((ticker, name))
    return ticker_list

def get_count_dict(company_list):
    import json
    import re
    # Build and return a name->count dict for downstream matching
    count_dict = {}
    for name in company_list:
        tokens = re.findall(r"\w+", (name or "").lower())
        for token in tokens:
            count_dict[token] = count_dict.get(token, 0) + 1
    #print(company_list)
    #print(count_dict)
    return company_list, count_dict

def match_company_names(ticker_list, company_list):
    #This function match (1:1)  every company in company_list with a unique ticker object(ticker_code, ticker_name) 
    from utils.string_matching_utils import get_label_and_confidence
    import re
    
    results = []
    
    
    to_match = [ticker for ticker in ticker_list]
    
    company_names = [company.get("name") for company in company_list]
    
    count_dict = get_count_dict(company_names)
    if (count_dict is None) or (len(count_dict) == 0):
        print("Count dictionary is empty. Cannot perform matching.")
        return results
    
    from tqdm import tqdm
    SINGLE_TOKEN_THRESHOLD = 3
    if to_match:
        for ticker in tqdm(to_match, desc="String matching", unit="company"):
            ticker_name = ticker[1]
            best_match = None
            best_confidence = 0
            for company in company_list:
                try:
                    company_name = company.get("name")
                    company_id = company.get("company_id")

                    label, confidence = get_label_and_confidence(entity_name=ticker_name, official_name=company_name, count_dict=count_dict, SINGLE_TOKEN_THRESHOLD=SINGLE_TOKEN_THRESHOLD)
                except Exception:
                    # If the helper fails for a candidate, skip it
                    continue
                if confidence is None:
                    continue
                if confidence == 100 or confidence == 100.0:
                    best_match_name = company_name
                    best_match_id = company_id
                    best_confidence = confidence
                    continue
                if confidence > best_confidence:
                    best_confidence = confidence
                    best_match_name = company_name
                    best_match_id = company_id
                    
            if best_match_name is None:
                best_match_name = None
                best_match_id = None
                best_confidence = 0.0
                
            results.append({
                "original_name": ticker_name,
                "matched_name": best_match_name,
                "matched_id": best_match_id ,
                "confidence": best_confidence,
                "ticker_code": ticker[0]
            })
            

            
            #remove the company name from company_list and update count_dict
            if best_match in company_list and best_confidence == 100:
                company_list.remove(best_match)
                tokens = re.findall(r"\w+", (best_match or "").lower())
                for tok in tokens:
                    if tok in count_dict:
                        count_dict[tok] -= 1
                        if count_dict[tok] <= 0:
                            del count_dict[tok]
            

    return results

def process_ticker_matching():
        tickers = scrape_sgx_ticker_list()
        
        #print the length of the ticker list
        print(f"Total tickers scraped: {len(tickers)}")
        
        from utils.db_utils import get_companies_to_ticker
        
        
        companies = list(get_companies_to_ticker())
        print(f"Total companies to match: {len(companies)}")
        

        results = match_company_names(tickers, companies)
        print(f"Total matched companies: {len(results)}")
        #print (results)
        
        from config.settings import PROJECT_ROOT 
        import csv
        data_dir = os.path.join(PROJECT_ROOT, "data")
        os.makedirs(data_dir, exist_ok=True)

        filename = f"matched_tickers.json"
        outfile = os.path.join(data_dir, filename)

        with open(outfile, "w", encoding="utf-8") as fh:
            json.dump(results, fh, ensure_ascii=False, indent=2)
            
        csv_filename = "matched_tickers.csv"
        csv_outfile = os.path.join(data_dir, csv_filename)

        with open(csv_outfile, "w", encoding="utf-8", newline="") as csvfh:
            fieldnames = ["ticker_code", "original_name", "matched_name", "confidence", "company_id"]
            writer = csv.DictWriter(csvfh, fieldnames=fieldnames)
            writer.writeheader()
            for r in results:
                writer.writerow({
                    "ticker_code": r.get("ticker_code", ""),
                    "original_name": r.get("original_name", ""),
                    "matched_name": r.get("matched_name", ""),
                    "confidence": r.get("confidence", ""),
                    "company_id": r.get("company_id", ""),
                })

        print(f"Wrote {len(results)} match records to {outfile}")

if __name__    == "__main__":

        from config.settings import PROJECT_ROOT 
        import csv
        data_dir = os.path.join(PROJECT_ROOT, "data")
        os.makedirs(data_dir, exist_ok=True)

        filename = "matched_tickers.json"
        outfile = os.path.join(data_dir, filename)

        if not os.path.exists(outfile):
            print(f"{outfile} not found. Starting process_ticker_matching()...")
            process_ticker_matching()
        else:
            print(f"File already exists: {outfile}")

        # Count how many objects in the JSON file have "confidence" == 100
        try:
            with open(outfile, "r", encoding="utf-8") as fh:
                data = json.load(fh)
                
                perfect_matches = [item for item in data if item.get("confidence") == 100 or item.get("confidence") == 100.0]
                print(f"Correct matches (confidence == 100): {len(perfect_matches)} / {len(data)} .")
                
                from utils.db_utils import add_ticker_info 
                for match in perfect_matches:
                    try:
                        ticker_code = match.get("ticker_code")
                        ticker_name = match.get("original_name")
                        company_id = match.get("matched_id")
                        if ticker_code and company_id:
                            add_ticker_info(company_id=company_id, ticker_code=ticker_code, ticker_name=ticker_name)
                    except Exception as e:
                        print(f"Error updating ticker info for company_id {company_id}, ticker_code {ticker_code}: {e}")
        except Exception as e:
            print(f"Error reading {outfile}: {e}")
        
        
        
        
        
