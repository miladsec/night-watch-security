import logging
import os
import time
import zipfile

from selenium import webdriver
from selenium.common import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from src.helpers.helper import get_today_string, is_utf8, remove_non_utf8_lines


class DailyJob:
    def __init__(self, data_folder, logger):
        self.logger = logger
        self.data_folder = data_folder

    def download_chaos_zip_file(self):
        if os.path.exists(os.path.join(self.data_folder, f'chaos-all-{get_today_string()}.zip')):
            return

        options = Options()
        options.set_preference("browser.download.folderList", 2)
        options.set_preference("browser.download.dir", self.data_folder)
        options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/zip")

        driver = webdriver.Firefox(options=options)

        driver.get("https://chaos.projectdiscovery.io/")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        element = driver.find_element(By.CSS_SELECTOR, "span .checkMark___3Hd8Q")
        driver.execute_script("arguments[0].scrollIntoView();", element)

        time.sleep(5)

        element.click()

        driver.find_element(By.CSS_SELECTOR, ".download_button___2WCKK").click()

        try:
            wait = WebDriverWait(driver, 600)  # 10 minutes
            wait.until(
                EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'Your file has been downloaded')]")))

        except TimeoutException:
            print("Timeout waiting for the download message.")

        finally:
            driver.quit()

    def extract_chaos_zip_file(self):
        if os.path.exists(os.path.join(self.data_folder, get_today_string())):
            return

        folder_path = os.path.join(self.data_folder, get_today_string())
        os.makedirs(folder_path)

        zip_file_path = os.path.join(self.data_folder, f'chaos-all-{get_today_string()}.zip')
        extract_to_directory = folder_path

        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to_directory)

    def cleanup_files_in_directory(self, tag=None):
        if os.path.exists(f'{os.path.join(self.data_folder, tag)}.cleaned'):
            return

        for root, dirs, files in os.walk(os.path.join(self.data_folder, tag)):
            for file in files:
                file_path = os.path.join(root, file)
                remove_non_utf8_lines(file_path)

        with open(f'{os.path.join(self.data_folder, tag)}.cleaned', 'w'):
            pass

    def read_utf8_lines(self, file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.read().splitlines()
                folder = os.path.basename(os.path.dirname(file_path))
                file_name = os.path.basename(file_path)

                return [{'Folder': folder, 'File': file_name, 'Data': line} for line in lines]
        except UnicodeDecodeError as e:
            self.logger.warning(f"UnicodeDecodeError in file {file_path}: {e}")
            return []

    def process_data_and_store(self, folder_name=None):
        root_directory = os.path.join(self.data_folder, folder_name)

        self.logger.info(f"Processing files in {root_directory}")

        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor() as executor:
            file_paths = [os.path.join(subdir, file) for subdir, _, files in os.walk(root_directory) for file in files]

            # Use executor.map to process files in parallel
            data_list = list(executor.map(self.read_utf8_lines, file_paths))

        # Concatenate DataFrames
        df = pd.concat([pd.DataFrame(data) for data in data_list], ignore_index=True)

        # Remove rows where 'Data' column starts with '*'
        df = df[~df['Data'].str.startswith('*')]

        grouped_df = df.groupby(['Folder', 'File']).agg({'Data': lambda x: list(x)}).reset_index()

        grouped_df.to_csv(os.path.join(self.data_folder, f'{folder_name}.csv'), index=False)

        return grouped_df

    def daily_job(self):
        start_time = time.time()

        try:
            self.download_chaos_zip_file()
            self.extract_chaos_zip_file()
            self.cleanup_files_in_directory(get_today_string())
            grouped_df = self.process_data_and_store(folder_name=get_today_string())
        except Exception as e:
            self.logger.error(f"An error occurred: {e}", exc_info=True)
        finally:
            end_time = time.time()
            execution_time = end_time - start_time
            self.logger.info(f"Execution Time: {execution_time} seconds")
