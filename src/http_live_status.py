import logging
import os
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

DATA_FOLDER = os.path.join(os.getcwd(), os.pardir, 'data')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def check_url_status(session, urls):
    logger.info(f"Checking {len(urls)} URLs")
    tasks = [check_single_url_status(session, url) for url in urls]
    results = await asyncio.gather(*tasks)
    return results


async def check_single_url_status(session, url):
    try:
        async with (session.head(f'http://{url}', timeout=0.1) as response_http,
                    session.head(f'https://{url}', timeout=0.1) as response_https):
            return response_http.status == 200 or response_https.status == 200
    except Exception as e:
        logger.error(f"Error checking URL {url}: {e}")
        return False


async def process_row(id, row, session, batch_size=1000):
    logger.info(f"Process for {id} started.")
    urls = eval(row["Data"])
    batches = [urls[i:i + batch_size] for i in range(0, len(urls), batch_size)]
    results = []

    for batch in batches:
        batch_results = await check_url_status(session, batch)
        results.extend(batch_results)

    logger.info(f"Process for urls {id} done.")
    return results


async def http_live_status(df, batch_size=1000):
    logger.info(f"Http live status checker started.")

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=100)) as session:
        with ThreadPoolExecutor() as executor:
            loop = asyncio.get_event_loop()
            tasks = [loop.create_task(process_row(id, row, session, batch_size)) for id, row in df.iterrows()]
            results = await asyncio.gather(*tasks)

    df["Status"] = results

    logger.info(f"Http live status checker Done.")

    logger.info(f"Http live status (FLAG) checker started.")
    df["AnyURLLive"] = df["Status"].apply(lambda x: any(x))
    logger.info(f"Http live status (FLAG) checker ended.")

    logger.info(f"Http live url lists started.")
    df["LiveURLs"] = df.apply(lambda row: [url for url, status in zip(eval(row["Data"]), row["Status"]) if status],
                              axis=1)
    logger.info(f"Http live url lists ended.")

    return df
