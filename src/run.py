import argparse
import asyncio
import os

import pandas as pd

from src.crons.daily_job import run_daily_job
from src.helpers.base import read_yaml_config, get_today_string
from src.http_live_status import http_live_status

DATA_FOLDER = os.path.join(os.getcwd(), 'data')
PD_DATA_FRAME = pd.read_csv(os.path.join(DATA_FOLDER, f'{get_today_string()}.csv')) if os.path.isfile(
    os.path.join(DATA_FOLDER, f'{get_today_string()}.csv')) else pd.DataFrame()


def add_args(parser):
    parser.add_argument('--version', action='store_true', help='Show the current version')
    parser.add_argument('--httplive', action='store_true', help='Looking for live http')
    parser.add_argument('--dailyjob', action='store_true', help='Daily data gathering and csv generator')


def main():
    parser = argparse.ArgumentParser()
    add_args(parser)
    args = parser.parse_args()

    base_configs = read_yaml_config()

    if args.version:
        print(f"you are using {base_configs.get('nws', {}).get('version')} version.")

    if args.httplive:
        print("looking for live http")
        results = asyncio.run(http_live_status(PD_DATA_FRAME))

    if args.dailyjob:
        run_daily_job()


if __name__ == '__main__':
    main()
