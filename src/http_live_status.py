import logging
import os
import asyncio
import httpx

DATA_FOLDER = os.path.join(os.getcwd(), os.pardir, 'data')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def check_url_status(url):
    logger.info(f"checking {url}")
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


async def process_row(id, row):
    logger.info(f"Process for {id} started.")
    urls = eval(row["Data"])
    results = await check_all_urls(urls)
    logger.info(f"Process for urls {id} done.")
    return results


async def http_live_status(df):
    logger.info(f"Http live status checker started.")
    df["Status"] = await asyncio.gather(*[process_row(id, row) for id, row in df.iterrows()])
    logger.info(f"Http live status checker Done.")

    logger.info(f"Http live status (FLAG) checker started.")
    df["AnyURLLive"] = df["Status"].apply(lambda x: any(x))
    logger.info(f"Http live status (FLAG) checker ended.")

    logger.info(f"Http live url lists started.")
    df["LiveURLs"] = df.apply(lambda row: [url for url, status in zip(eval(row["Data"]), row["Status"]) if status],
                              axis=1)
    logger.info(f"Http live url lists ended.")

    return df
