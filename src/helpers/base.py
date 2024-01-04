from datetime import datetime


def get_today_string():
    now = datetime.now()
    current_date = f"{now.day}-{now.month}-{now.year}"
    return current_date
