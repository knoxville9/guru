import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# Define max_workers as a global variable
MAX_WORKERS = 1500

def query_gurufocus_history(stock_code: str, market_prefix: str, authorization: str, cookie: str, signature: str):
    url = f"https://www.gurufocus.com/reader/_api/gf_rank/{market_prefix}{stock_code}?v=1.7.44"
    headers = {
        "accept": "application/json, text/plain, /",
        "accept-language": "zh-CN,zh;q=0.9",
        "authorization": authorization,
        "priority": "u=1, i",
        "referer": "https://www.gurufocus.com/stocks/region/asia/china",
        "sec-ch-ua": '"Chromium";v="139", "Not;A=Brand";v="99"',
        "sec-ch-ua-arch": '""',
        "sec-ch-ua-bitness": '""',
        "sec-ch-ua-full-version": '""',
        "sec-ch-ua-full-version-list": "",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-model": '""',
        "sec-ch-ua-platform": '"Windows"',
        "sec-ch-ua-platform-version": '""',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "signature": signature,
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
    }
    cookies = dict([c.strip().split('=', 1) for c in cookie.split(';') if '=' in c])
    response = requests.get(url, headers=headers, cookies=cookies, verify=False)
    return stock_code, response

def batch_request_save_filtered(authorization: str, cookie: str, signature: str):
    tasks = []
    # SHSE 603001 - 605599
    for i in range(603001, 605600):
        stock_code = str(i).zfill(6)
        tasks.append(('SHSE:', stock_code))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for market_prefix, stock_code in tasks:
            futures.append(executor.submit(query_gurufocus_history, stock_code, market_prefix, authorization, cookie, signature))
        for future in as_completed(futures):
            try:
                stock_code, response = future.result()
                if response.status_code == 200:
                    data = response.json()
                    rank = data.get('rank', None)
                    if rank is not None and rank < 90:
                        print(f"Stock code: {stock_code} rank<{rank}, skipping save.")
                        continue
                    filename = f"{stock_code}.json"
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    print(f"Saved: {filename}  Response length: {len(response.text)}")
                else:
                    print(f"Request failed, stock code: {stock_code}, status code: {response.status_code}")
            except Exception as e:
                print(f"Request error, stock code: {stock_code}, error: {e}")

authorization = ""
cookie = ""
signature = ""

batch_request_save_filtered(authorization, cookie, signature)
