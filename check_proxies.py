import requests, time
from concurrent.futures import ThreadPoolExecutor, as_completed

stock_code = "00700"
TEST_URL = "https://emweb.securities.eastmoney.com/PC_HKF10/pages/home/index.html?code={stock_code}&type=web&color=w#/NewFinancialAnalysis"
TIMEOUT = 6

def _make_requests_proxies(proxy: str) -> dict:
    # Accepts "ip:port" or "user:pass@ip:port"
    proxy_url = f"http://{proxy}"
    proxies = {"http": proxy_url, "https": proxy_url}
    start = time.time()
    try:
        r = requests.get(TEST_URL, proxies=proxies, timeout=TIMEOUT)
        latency = time.time() - start
        return {"proxy": proxy, "ok": r.status_code == 200, "latency": latency, "status": r.status_code}
    except Exception as e:
        return {"proxy": proxy, "ok": False, "latency": None, "error": str(e)}

def validate_proxies(proxy_list, max_workers=30):
    good, bad = [], []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_make_requests_proxies, p) for p in proxy_list]
        for fut in as_completed(futures):
            res = fut.result()
            if res["ok"]:
                good.append(res)
            else:
                bad.append(res)
    return good, bad

def save_proxies(path, proxies):
    with open(path, "w") as f:
        for p in proxies:
            f.write(p["proxy"] + "\n")

if __name__ == "__main__":
    with open("proxies_list.txt", "r") as f:
        proxy_list = [line.strip() for line in f if line.strip()][3:]
    # 1st time
    good, bad = validate_proxies(proxy_list)
    print(f"After 1st test - Valid proxies: {len(good)}, Invalid proxies: {len(bad)}")
    # 2nd time for bad proxies
    if bad:
        print(f"Re-testing {len(bad)} bad proxies...")
        good2, bad2 = validate_proxies(bad)
        good.extend(good2)
        bad = bad2
    print(f"After 2nd test - Valid proxies: {len(good)}, Invalid proxies: {len(bad)}")
    save_proxies("valid_proxies_2.txt", good)