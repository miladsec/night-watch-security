# db_connection.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.helpers.helper import read_config_yml

CONFIG_YML = read_config_yml()
engine = create_engine(CONFIG_YML.get('postgresql', {}).get('connection_string'))

Session = sessionmaker(bind=engine)
session = Session()
