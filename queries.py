from sqlalchemy import inspect
from tabulate import tabulate

from db.database import get_session, exec_query
from db.db_1_computer_firm.models import Product, PC, Laptop, Printer


class SessionCreater:
    """
    Базовый класс для создания сессий SQLAlchemy.

    Этот класс позволяет управлять сессиями SQLAlchemy, обеспечивая их создание и закрытие
    в контексте оператора `with`. Он предоставляет удобный способ взаимодействия с базой данных,
    а также автоматически закрывает сессию после завершения блока `with`.
    """

    def __enter__(self):
        """Создает и возвращает новую сессию SQLAlchemy внутри контекстного блока `with`."""
        self.session = get_session()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Закрывает сессию SQLAlchemy при выходе из контекстного блока `with`.

        Args:
            exc_type (type): Тип исключения, если такое произошло внутри блока `with`.
            exc_value (Exception): Исключение, если такое произошло внутри блока `with`.
            traceback (traceback): Трейс исключения, если такое произошло внутри блока `with`.
        """
        self.session.close()


class ComputerFirmTasks(SessionCreater):
    """
    Класс для решения задач по первой БД (компьютерная фирма)
    """

    def show_pc_table(self):
        # Запрашиваем все записи из таблицы PC
        pcs = self.session.query(PC).all()

        # Используем инспектор SQLAlchemy для получения информации о структуре таблицы
        table_columns = inspect(self.session.bind).get_columns('pc')  # 'pc' - имя таблицы

        # Преобразуем данные в формат, подходящий для tabulate
        headers = [column['name'] for column in table_columns]
        table_data = [(pc.__dict__[column] for column in headers) for pc in pcs]
        print(tabulate(table_data, headers, tablefmt="pretty"))

    def task_1(self):
        # Фильтруем ПК по стоимости менее 500 долларов и выбираем необходимые столбцы
        pcs = self.session.query(PC.model_id, PC.speed, PC.hd).filter(PC.price < 500.0).all()

        table_data = [(pc.model_id, pc.speed, pc.hd) for pc in pcs]
        headers = ['model', 'speed', 'hd']
        print("Task #1 (SQL-Alchemy):")
        print(tabulate(table_data, headers, tablefmt='pretty'))

    def task_1_postgre(self):
        query = """
        SELECT model_id, speed, hd
        FROM pc
        WHERE price < 500.0
        ORDER BY model_id, speed
        """

        result = exec_query(query)

        if result:
            headers = ['model', 'speed', 'hd']
            print("Task #1 (PostgreSQL):")
            print(tabulate(result, headers, tablefmt='pretty'))
        else:
            print("No results found")


class RecyclingFirmTasks(SessionCreater):
    pass


if __name__ == '__main__':
    # Сессия будет автоматически закрыта после выхода из блока with
    with ComputerFirmTasks() as comp_firm_task:
        # Выполняем задачи
        comp_firm_task.task_1()
        comp_firm_task.task_1_postgre()
