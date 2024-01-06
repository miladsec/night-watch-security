import argparse
import asyncio
import concurrent.futures
import logging
import os

import pandas as pd

from src.helpers.helper import read_base_yaml, get_today_string

from src.subfunctions.daily_job.daily_job import DailyJob
from src.subfunctions.live_http.live_http import HttpLive
from src.subfunctions.version.version import Version


def process_chunk(chunk, live_http):
    result_df = asyncio.run(live_http.live_http(chunk))
    return result_df


class NightWatchSecurity:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description='Night Watch Security CLI Application')
        self.parser.add_argument("-v", "--version", action="store_true", help="Show version")
        self.parser.add_argument("-dj", "--dailyjob", action='store_true',
                                 help='Daily data gathering and csv generator')
        self.parser.add_argument("-lh", "--livehttp", action="store_true", help="find lives http targets")
        self.parser.add_argument("-c", "--chunksize", type=int, default=1000,
                                 help="Specify the chunk size for processing the DataFrame")
        self.base_configs = read_base_yaml()

        self.data_folder = os.path.join(os.getcwd(), 'data')

        self.pd_data_frame = pd.read_csv(os.path.join(self.data_folder, f'{get_today_string()}.csv')) if os.path.isfile(
            os.path.join(self.data_folder, f'{get_today_string()}.csv')) else pd.DataFrame()

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def run(self, args):
        if args.version:
            self.version()
        elif args.dailyjob:
            self.daily_job()
        elif args.livehttp:
            self.live_http(args.chunksize)
        else:
            print("Invalid command")

    def version(self):
        version = Version(self.base_configs)
        version.show_version()

    def daily_job(self):
        daily_job = DailyJob(self.data_folder, self.logger)
        daily_job.daily_job()

    def live_http(self, chunk_size):
        # live_http = HttpLive(self.data_folder, self.logger)
        # asyncio.run(live_http.live_http(self.pd_data_frame))
        # TODO::Only check for performance (try to fix memory exception)

        live_http = HttpLive(self.data_folder, self.logger)

        chunks = pd.read_csv(os.path.join(self.data_folder, f'{get_today_string()}.csv'), chunksize=chunk_size)

        # num_threads = max(1, os.cpu_count() - 1)

        final_result_df = pd.DataFrame()

        # with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        #     # Submit processing tasks for each chunk
        #     futures = [executor.submit(process_chunk, chunk, live_http) for chunk in chunks]
        #
        #     # Wait for all tasks to complete
        #     concurrent.futures.wait(futures)
        #
        #     # Get results from completed tasks
        #     for future in concurrent.futures.as_completed(futures):
        #         result_df = future.result()
        #         final_result_df = pd.concat([final_result_df, result_df], ignore_index=True)

        # TODO:: only keep old method to rollback easier
        for chunk in chunks:
            result_df = asyncio.run(live_http.live_http(chunk))
            final_result_df = pd.concat([final_result_df, result_df], ignore_index=True)

        final_result_df.to_csv(os.path.join(self.data_folder, f'{get_today_string()}_live.csv'), index=False)


if __name__ == '__main__':
    cli = NightWatchSecurity()
    args = cli.parser.parse_args()
    cli.run(args)
