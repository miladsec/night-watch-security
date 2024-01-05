import os
from datetime import datetime
import yaml


def get_today_string():
    now = datetime.now()
    current_date = f"{now.day}-{now.month}-{now.year}"
    return current_date


def is_utf8(encoded_text):
    try:
        decoded_text = encoded_text.decode('utf-8')
        return True
    except UnicodeDecodeError:
        return False


def remove_non_utf8_lines(file_path):
    with open(file_path, 'rb') as file:
        lines = file.readlines()

    utf8_lines = [line for line in lines if is_utf8(line)]

    with open(file_path, 'wb') as file:
        file.writelines(utf8_lines)


def read_base_yaml():
    base_path = os.path.join(os.getcwd(), 'src', 'config', 'base.yml')
    with open(base_path, 'r') as file:
        config = yaml.safe_load(file)
    return config


def read_config_yml():
    base_path = os.path.join(os.getcwd(), 'src', 'config', 'config.yml')
    with open(base_path, 'r') as file:
        config = yaml.safe_load(file)
    return config
