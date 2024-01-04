import os
import asyncio
import httpx

DATA_FOLDER = os.path.join(os.getcwd(), os.pardir, 'data')


async def check_url_status(url):
    async with httpx.AsyncClient() as client:
        try:
            response_http = await client.head('http://' + url)
            response_https = await client.head('https://' + url)

            return response_http.is_success or response_https.is_success
        except Exception as e:
            print(f"Error checking URL {url}: {e}")
            return False


async def check_all_urls(urls):
    tasks = [check_url_status(url) for url in urls]
    results = await asyncio.gather(*tasks)
    return results


async def process_row(row):
    urls = eval(row["Data"])  # Convert string representation of list to actual list
    results = await check_all_urls(urls)
    return results


async def http_live_status(df):
    df["Status"] = await asyncio.gather(*[process_row(row) for _, row in df.iterrows()])
    df["AnyURLLive"] = df["Status"].apply(lambda x: any(x))
    df["LiveURLs"] = df.apply(lambda row: [url for url, status in zip(eval(row["Data"]), row["Status"]) if status],
                              axis=1)
    return df
