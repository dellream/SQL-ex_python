from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db.config import DB_PASSWORD, DB_USER, DB_HOST, DB_PORT, DB_NAME_COMPUTER_FIRM

engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME_COMPUTER_FIRM}')

# Создаем сессию
Session = sessionmaker(bind=engine)


def get_session():
    """Возвращает новую сессию соединения с базой данных"""
    return Session()
