from typing import Any, Dict, Optional
import requests
import random

from config.settings import CMS_URL

# In-memory token cache similar to the client-side _token
_CACHED_SGX_TOKEN: Optional[str] = None

# Header di default che possono essere sovrascritti passando un dict a get_headers
DEFAULT_HEADERS: Dict[str, str] = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6",
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

    "authorizationtoken": None, # Placeholder; will be set in get_headers
    # priority header is unusual; include it as-is if desired
    "priority": "u=1, i",
}

# List of User-Agent strings to simulate different browsers
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:102.0) Gecko/20100101 Firefox/102.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:102.0) Gecko/20100101 Firefox/102.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/114.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Edge/114.0.0.0",
]

def get_headers(overrides: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """
    Returns a copy of DEFAULT_HEADERS overridden with the values in `overrides`.
    Rotates the User-Agent header to simulate different browsers.
    Example: get_headers({"Authorization": "Bearer token"})
    """
    headers = DEFAULT_HEADERS.copy()
    if overrides:
        headers.update(overrides)
    try:
        headers["authorizationtoken"] = fetch_sgx_token()
    except Exception as e:
        print(f"Error fetching SGX token: {e}")
        headers["authorizationtoken"] = ""  # Fallback to empty token

    # Rotate User-Agent
    headers["User-Agent"] = random.choice(USER_AGENTS)

    return headers

def fetch_sgx_token(cms_url: str = CMS_URL,
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
            raise ValueError("Token not found in response data")
        # apply ROT13: Python's codecs supports rot_13
        import codecs

        token = codecs.decode(token_enc, "rot_13")
        _CACHED_SGX_TOKEN = token
        return token
    except requests.exceptions.RequestException as req_err:
        raise Exception(f"HTTP error while fetching SGX token: {req_err}")
    except ValueError as val_err:
        raise Exception(f"Data error while fetching SGX token: {val_err}")
    except Exception as e:
        raise Exception(f"Unexpected error while fetching SGX token: {e}")