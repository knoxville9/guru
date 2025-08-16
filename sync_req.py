import asyncio
import aiohttp
import aiofiles
import json
import ssl
import time
import os
from typing import Tuple, Optional

# Configurable parameters
CONCURRENCY = 100               # Number of concurrent requests (adjust according to machine and target server capacity)
RETRIES = 3                     # Number of retries per request
BACKOFF_FACTOR = 0.5            # Exponential backoff base factor
REQUEST_TIMEOUT = 15            # Single request timeout in seconds
OUTPUT_FILE = "output.jsonl"    # Save results to a single JSONL file to avoid many small files
SAVE_SEPARATE_FILES = False     # If True, create a separate file for each stock_code (preserve original behavior)
VERIFY_SSL = True               # Whether to verify SSL certificates; recommended True in production
LIMIT_PER_HOST = 0              # aiohttp.TCPConnector limit_per_host (0 means no per-host limit)
START = 603001
END = 605599 + 1                # range upper bound +1

# Headers template (you can supply authorization/signature/cookie to replace or extend these)
COMMON_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "zh-CN,zh;q=0.9",
    "priority": "u=1, i",
    "referer": "https://www.gurufocus.com/stocks/region/asia/china",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# Helper: parse cookie string -> dict
def parse_cookie_str(cookie_str: str) -> dict:
    return dict([c.strip().split('=', 1) for c in cookie_str.split(';') if '=' in c])

# Build SSL context. Return None to use aiohttp default verification.
def make_ssl_context(verify: bool) -> Optional[ssl.SSLContext]:
    if verify:
        return None  # aiohttp default will perform verification
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx

# Single fetch with retry and exponential backoff
async def fetch_one(session: aiohttp.ClientSession, stock_code: str, market_prefix: str,
                    headers: dict, cookies: dict, sem: asyncio.Semaphore) -> Tuple[str, Optional[dict], Optional[str]]:
    url = f"https://www.gurufocus.com/reader/_api/gf_rank/{market_prefix}{stock_code}?v=1.7.44"
    attempt = 0
    async with sem:
        while attempt <= RETRIES:
            try:
                timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                async with session.get(url, headers=headers, cookies=cookies, timeout=timeout) as resp:
                    status = resp.status
                    text = await resp.text()
                    if status == 200:
                        # Try to parse JSON
                        try:
                            data = await resp.json()
                        except Exception:
                            data = None
                        return stock_code, data, text
                    # Retry on common transient statuses with backoff
                    if status in (429, 500, 502, 503, 504):
                        wait = BACKOFF_FACTOR * (2 ** attempt)
                        attempt += 1
                        await asyncio.sleep(wait)
                        continue
                    # Other statuses: return with error message
                    return stock_code, None, f"status {status}"
            except asyncio.TimeoutError:
                attempt += 1
                await asyncio.sleep(BACKOFF_FACTOR * (2 ** attempt))
            except Exception as e:
                attempt += 1
                await asyncio.sleep(BACKOFF_FACTOR * (2 ** attempt))
        return stock_code, None, "failed after retries"

# Batch runner: process tasks and write outputs
async def run_batch(authorization: str, cookie: str, signature: str):
    # Prepare headers & cookies
    headers = COMMON_HEADERS.copy()
    if authorization:
        headers["authorization"] = authorization
    if signature:
        headers["signature"] = signature
    cookies = parse_cookie_str(cookie) if cookie else {}

    ssl_ctx = make_ssl_context(VERIFY_SSL)

    connector = aiohttp.TCPConnector(limit=CONCURRENCY, limit_per_host=LIMIT_PER_HOST, ssl=ssl_ctx)
    sem = asyncio.Semaphore(CONCURRENCY)

    # Ensure output folder exists if saving separate files
    if SAVE_SEPARATE_FILES and not os.path.isdir("out"):
        os.makedirs("out", exist_ok=True)

    total = END - START
    success_count = 0
    skipped_count = 0
    failed_count = 0
    processed = 0

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for i in range(START, END):
            stock_code = str(i).zfill(6)
            # The original script used ('SHSE:', stock_code) as market_prefix; extend here if you have more markets
            tasks.append(fetch_one(session, stock_code, "SHSE:", headers, cookies, sem))

        # Process results as they arrive
        for coro in asyncio.as_completed(tasks):
            stock_code, data, text_or_err = await coro
            processed += 1

            if data:
                rank = data.get("rank", None)
                # Original script logic: if rank is not None and rank < 90: skip saving
                if rank is not None and rank < 90:
                    print(f"[{processed}/{total}] {stock_code} rank={rank} -> skipped")
                    skipped_count += 1
                    continue
                # Save data
                if SAVE_SEPARATE_FILES:
                    fname = os.path.join("out", f"{stock_code}.json")
                    try:
                        async with aiofiles.open(fname, "w", encoding="utf-8") as f:
                            await f.write(json.dumps(data, ensure_ascii=False, indent=2))
                        print(f"[{processed}/{total}] Saved file {fname}")
                    except Exception as e:
                        print(f"[{processed}/{total}] Write error for {stock_code}: {e}")
                        failed_count += 1
                else:
                    # Append JSON line to output file
                    try:
                        async with aiofiles.open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                            await f.write(json.dumps({"code": stock_code, "data": data}, ensure_ascii=False) + "\n")
                        success_count += 1
                        print(f"[{processed}/{total}] Appended {stock_code}  len={len(text_or_err) if isinstance(text_or_err, str) else 'N/A'}")
                    except Exception as e:
                        print(f"[{processed}/{total}] Append error for {stock_code}: {e}")
                        failed_count += 1
            else:
                failed_count += 1
                print(f"[{processed}/{total}] Failed {stock_code}: {text_or_err}")

    print("Done. processed:", processed, "saved:", success_count, "skipped:", skipped_count, "failed:", failed_count)

# Entrypoint helper
def main():
    authorization = ""  # Fill in your authorization token if needed
    cookie = ""         # Fill in your cookie string if needed
    signature = ""      # Fill in your signature if needed
    start_time = time.time()
    asyncio.run(run_batch(authorization, cookie, signature))
    print("Elapsed:", time.time() - start_time)

if __name__ == "__main__":
    main()
