import psycopg2

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db.config import (
    DB_PASSWORD,
    DB_USER,
    DB_HOST,
    DB_PORT,
    DB_NAME
)

engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Создаем сессию
Session = sessionmaker(bind=engine)


def get_session():
    """Возвращает новую сессию соединения с базой данных"""
    return Session()


def exec_query(query):
    """
    Создает подключение к БД, используя psycopg2 в качестве драйвера.
    Возвращает результат запроса из БД
    """
    # Подключаемся к PostgreSQL
    connection = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    # Создаем курсор для отправки SQL-запросов базе данных
    cursor = connection.cursor()
    # Выполняет SQL-запрос, переданный как аргумент функции
    cursor.execute(query)
    # Сохраняем в переменной все строки результата запроса
    results = cursor.fetchall()

    # Закрываем курсор и соединение
    cursor.close()
    connection.close()

    return results
