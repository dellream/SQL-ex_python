import os

from dotenv import load_dotenv

load_dotenv()

# Переменные для подключения к Postgresql
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
DB_NAME_COMPUTER_FIRM = os.environ.get('DB_NAME_COMPUTER_FIRM')
DB_NAME = os.environ.get('DB_NAME')
# DB_NAME_RECYCLING_FIRM = os.environ.get('DB_NAME_RECYCLING_FIRM')
# DB_NAME_SHIPS = os.environ.get('DB_NAME_SHIPS')
# DB_NAME_AIRPORT = os.environ.get('DB_NAME_AIRPORT')
# DB_NAME_PAINTING = os.environ.get('DB_NAME_PAINTING')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')