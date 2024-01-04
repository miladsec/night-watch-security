import os
from datetime import datetime
import yaml


def get_today_string():
    now = datetime.now()
    current_date = f"{now.day}-{now.month}-{now.year}"
    return current_date


def read_yaml_config():
    base_path = os.path.join(os.getcwd(), 'src', 'config', 'base.yml')
    with open(base_path, 'r') as file:
        config = yaml.safe_load(file)
    return config
