# This script scrape the ticker list from the SGX website (ticker - ticker_name) and then match the ticker name with the official company name the database and then update metadata (uat/prod) accordingly

import requests
from bs4 import BeautifulSoup
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
        ticker = item.get("cur")
        company_name = item.get("n")
        if ticker and company_name:
            ticker_list.append((ticker, company_name))
    return ticker_list

if __name__    == "__main__":
        tickers = scrape_sgx_ticker_list()
        for ticker, name in tickers:
            print(f"{ticker} - {name}")