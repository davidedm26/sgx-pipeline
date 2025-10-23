"""Safer scraper utilities for SGX announcements.

This module provides small helpers to call the SGX announcements API
without embedding sensitive values directly into URLs or module-level
constants that may not be defined in the runtime environment.

Note: keep tokens out of source control. Load them from environment
variables or a secrets manager when running for real.
"""

from typing import Any, Dict, Optional
import copy
import requests
from bs4 import BeautifulSoup
import json as _json



SGX_URL = "https://www.sgx.com/"
SGX_COMPANY_API_URL = "https://api.sgx.com/announcements/v1.1/company"
SGX_RESULTS_COUNT_API_URL="https://api.sgx.com/announcements/v1.1/company/count"

# Header di default che possono essere sovrascritti passando un dict a get_headers
DEFAULT_HEADERS: Dict[str, str] = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 OPR/122.0.0.0",
    "X-Requested-With": "XMLHttpRequest",
    "authority": "api.sgx.com",
    "origin": "https://www.sgx.com",
    "referer": "https://www.sgx.com/",
    "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Opera";v="122"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",

    "authorizationtoken": "47tkBGXKPtddY+1s65NLhtKBNP0TvS95pJi4lLJTFpgfZE+EvGXITvpy1bEUYsk5p9hlwNkFM6A34xE13YDaRQ8qEUixqKFZ24W8o7XkZlfBkZ7uukoykgmFJV3WtftrCyEstwPBnN==",
    # priority header is unusual; include it as-is if desired
    "priority": "u=1, i",
}


def get_headers(overrides: Optional[Dict[str, str]] = None,
                authorization_token: Optional[str] = None,
                origin: Optional[str] = None,
                referer: Optional[str] = None) -> Dict[str, str]:
    """
    Restituisce una copia dei DEFAULT_HEADERS sovrascritta con i valori in `overrides`.
    Esempio: get_headers({"User-Agent": "CustomAgent/1.0"})
    """
    headers = DEFAULT_HEADERS.copy()
    if overrides:
        headers.update(overrides)
    # Allow reading the token from environment if not explicitly passed
    '''
    if not authorization_token:
        import os

        authorization_token = os.environ.get("SGX_AUTH_TOKEN")
    if authorization_token:
        # The API in your capture uses header name 'authorizationtoken'
        headers["authorizationtoken"] = authorization_token
    '''
    headers["authorizationtoken"] = _CACHED_SGX_TOKEN
    # Keep header names lowercase for consistency with capture; allow overrides
    if origin:
        headers["origin"] = origin
    if referer:
        headers["referer"] = referer
    return headers


# In-memory token cache similar to the client-side _token
_CACHED_SGX_TOKEN: Optional[str] = None


def fetch_sgx_token(cms_url: str = "https://api2.sgx.com/content-api/?queryId=17d94f69435775a0d673d1b5328b0403ce4ad025:we_chat_qr_validator",
                    session: Optional[requests.Session] = None,
                    timeout: int = 10) -> Optional[str]:
    """Fetches the qrValidator from the CMS endpoint and applies ROT13.

    Returns the decoded token or None on failure. Caches the token in module-level variable to avoid repeated calls.
    """
    global _CACHED_SGX_TOKEN
    if _CACHED_SGX_TOKEN:
        return _CACHED_SGX_TOKEN

    s = session or requests.Session()
    try:
        resp = s.get(cms_url, timeout=timeout)
        resp.raise_for_status()
        body = resp.json()
        data = body.get("data", {}) or {}
        token_enc = data.get("qrValidator") or data.get("qrvalidator")
        if not token_enc:
            return None
        # apply ROT13: Python's codecs supports rot_13
        import codecs

        token = codecs.decode(token_enc, "rot_13")
        _CACHED_SGX_TOKEN = token
        return token
    except Exception:
        return None


def search_company_data(company_name: str = "DBS GROUP HOLDINGS LTD",
                        periodstart: Optional[str] = "20250204_160000",
                        periodend: Optional[str] = "20251022_120000",
                        exactsearch: bool = True,
                        pagestart: int = 0,
                        pagesize: int = 20) -> None:
    """Search announcements for a company and save two responses (list & count).

    By default uses the previous hardcoded example values. Returns None and
    writes results to files named '0_sgx_announcements.html' and '1_sgx_announcements.html'.
    """
    params = {
        "periodstart": periodstart,
        "periodend": periodend,
        "value": company_name,
        "exactsearch": str(exactsearch).lower(),
        "pagestart": pagestart,
        "pagesize": pagesize,
    }

    documents_response = requests.get(SGX_COMPANY_API_URL, params=params, headers=get_headers())

    count_params = {k: v for k, v in params.items() if k != "pagesize" and k != "pagestart"} # count endpoint expects same params except pagesize/pagestart 
    print(count_params)
    
    count_response = requests.get(SGX_RESULTS_COUNT_API_URL, params=count_params, headers=get_headers())

    if documents_response.raise_for_status() or count_response.raise_for_status():  # Raise an error for bad responses
        print("Error during HTTP GET requests")
        return

    for idx,response in enumerate([documents_response, count_response]):
        try:
            data = response.json()
            # Convert JSON to a simple HTML representation and prettify with BeautifulSoup
            html = "<html><body><pre>{}</pre></body></html>".format(
                _json.dumps(data, indent=2, ensure_ascii=False)
            )
            soup = BeautifulSoup(html, "html.parser")
            filename = f"{idx}_sgx_announcements.html"
            with open(filename, "w", encoding="utf-8") as f:
                f.write(soup.prettify())
            print(f"Saved response to {filename}")
            
        except ValueError:
            # Treat response as HTML/text; parse and prettify
            soup = BeautifulSoup(response.text, "html.parser")
            print(soup.prettify())
            data = soup
    


if __name__ == "__main__":
    token = fetch_sgx_token()
    print(f"Fetched SGX token: {token}")
    response = search_company_data()