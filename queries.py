from sqlalchemy import inspect
from tabulate import tabulate

from db.database import get_session
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

    def task_1(self):
        ...


class RecyclingFirmTasks(SessionCreater):
    pass


if __name__ == '__main__':
    # Сессия будет автоматически закрыта после выхода из блока with
    with ComputerFirmTasks() as comp_firm_task:
        # Выполняем задачи
        comp_firm_task.task_1()
