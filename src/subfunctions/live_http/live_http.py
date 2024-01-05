import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor


class HttpLive:
    def __init__(self, data_folder, logger):

        self.logger = logger
        self.data_folder = data_folder

    async def check_url_status(self, session, urls):
        self.logger.info(f"Checking {len(urls)} URLs")
        tasks = [self.check_single_url_status(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        return results

    async def check_single_url_status(self, session, url):
        try:
            async with (session.head(f'http://{url}', timeout=5) as response_http,
                        session.head(f'https://{url}', timeout=5) as response_https):
                return 200 <= response_http.status < 400 or 200 <= response_https.status < 400
        except Exception as e:
            self.logger.error(f"Error checking URL {url}: {e}")
            return False

    async def process_row(self, id, row, session, batch_size=1000):
        self.logger.info(f"Process for {id} started.")
        urls = eval(row["Data"])
        batches = [urls[i:i + batch_size] for i in range(0, len(urls), batch_size)]
        results = []

        for batch in batches:
            batch_results = await self.check_url_status(session, batch)
            results.extend(batch_results)

        self.logger.info(f"Process for urls {id} done.")
        return results

    async def live_http(self, df, batch_size=1000):
        self.logger.info(f"Http live status checker started.")

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=100)) as session:
            with ThreadPoolExecutor() as executor:
                loop = asyncio.get_event_loop()
                tasks = [loop.create_task(self.process_row(id, row, session, batch_size)) for id, row in df.iterrows()]
                results = await asyncio.gather(*tasks)

        df["Status"] = results

        self.logger.info(f"Http live status checker Done.")

        self.logger.info(f"Http live status (FLAG) checker started.")
        df["AnyURLLive"] = df["Status"].apply(lambda x: any(x))
        self.logger.info(f"Http live status (FLAG) checker ended.")

        self.logger.info(f"Http live url lists started.")
        df["LiveURLs"] = df.apply(lambda row: [url for url, status in zip(eval(row["Data"]), row["Status"]) if status],
                                  axis=1)
        self.logger.info(f"Http live url lists ended.")

        return df
