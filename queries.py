from sqlalchemy import (
    inspect,
    distinct,
    select,
    or_,
    union,
    except_,
    func,
    and_,
    intersect,
    union_all,
    all_,
    cte
)
from sqlalchemy.orm import aliased
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
        query = self.session.query(
            PC.model_id,
            PC.speed,
            PC.hd
        ).filter(PC.price < 500.0).all()

        headers = ['model', 'speed', 'hd']
        print("Task #1 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

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

    def task_2(self):
        """Найдите производителей принтеров. Вывести: maker"""
        query = self.session.execute(
            select(distinct(Product.maker))
            .filter_by(type='Printer')
        ).all()

        headers = ['maker']
        print("Task #2 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_2_postgre(self):
        query = """
        SELECT DISTINCT maker
        FROM product
        WHERE type = 'Printer'
        """

        result = exec_query(query)
        if result:
            headers = ['maker']
            print("Task #2 (PostgreSQL):")
            print(tabulate(result, headers, tablefmt='pretty'))
        else:
            print("No results found")

    def task_3(self):
        """
        Найдите номер модели, объем памяти и размеры экранов ПК-блокнотов,
        цена которых превышает 1000 дол.
        """
        query = self.session.execute(
            select(
                Laptop.model_id,
                Laptop.ram,
                Laptop.screen
            )
            .filter(Laptop.price > 1000)
            .order_by(Laptop.model_id)
        ).all()

        headers = ['model', 'ram', 'screen']
        print("Task #3 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_4(self):
        """Найдите все записи таблицы Printer для цветных принтеров."""
        query = self.session.execute(
            select(
                Printer.code,
                Printer.model_id,
                Printer.color,
                Printer.type,
                Printer.price
            )
            .filter(Printer.color == 'y')
        ).all()

        headers = [column.name for column in Printer.__table__.columns]
        print("Task #4 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_5(self):
        """
        Найдите номер модели, скорость и размер жесткого диска ПК,
        имеющих 12x или 24x CD и цену менее 600 дол.
        """

        query = self.session.execute(
            select(
                PC.model_id,
                PC.speed,
                PC.hd
            )
            .filter(or_(PC.cd == '12x', PC.cd == '24x')
                    & (PC.price < 600))
        ).all()

        headers = ['model', 'speed', 'hd']
        print("Task #5 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_6(self):
        """
        Для каждого производителя, выпускающего ПК-блокноты c объёмом жесткого диска
        не менее 10 Гбайт, найти скорости таких ПК-блокнотов. Вывод: производитель, скорость.
        """
        lap = aliased(Laptop)
        prod = aliased(Product)

        query = self.session.query(
            distinct(prod.maker),
            lap.speed
        ).join(prod, lap.model_id == prod.model) \
            .filter(lap.hd >= 10).all()

        headers = ['maker', 'speed']
        print("Task #6 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_7(self):
        """
        Найдите номера моделей и цены всех имеющихся в продаже
        продуктов (любого типа) производителя B (латинская буква).
        """

        query = self.session.execute(
            union(
                select(
                    distinct(PC.model_id),
                    PC.price
                )
                .join(Product, PC.model_id == Product.model)
                .filter(Product.maker == 'B'),

                select(
                    distinct(Product.model),
                    Laptop.price
                )
                .join(Laptop, Laptop.model_id == Product.model)
                .filter(Product.maker == 'B'),

                select(
                    distinct(Product.model),
                    Printer.price
                )
                .join(Printer, Printer.model_id == Product.model)
                .filter(Product.maker == 'B')
            )
        )

        headers = ['model', 'price']
        print("Task #7 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_8(self):
        """Найдите производителя, выпускающего ПК, но не ПК-блокноты."""
        query = self.session.execute(
            except_(
                select(Product.maker)
                .filter(Product.type == 'PC'),

                select(Product.maker)
                .filter(Product.type == 'Laptop')
            )
        )
        headers = ['maker']
        print("Task #8 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_9(self):
        """Найдите производителей ПК с процессором не менее 450 Мгц. Вывести: Maker"""
        query = self.session.execute(
            select(
                distinct(Product.maker)
            )
            .join(PC, Product.model == PC.model_id)
            .filter(PC.speed >= 450)
            .order_by(Product.maker)
        )

        headers = ['maker']
        print("Task #9 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_10(self):
        """Найдите модели принтеров, имеющих самую высокую цену. Вывести: model, price"""
        query = self.session.execute(
            select(
                Printer.model_id,
                Printer.price
            )
            .filter(Printer.price == select(func.max(Printer.price)).scalar_subquery())
        ).all()

        headers = ['model', 'price']
        print("Task #10 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_11(self):
        """Найдите среднюю скорость ПК."""
        query = self.session.execute(
            select(func.avg(PC.speed))
        )

        headers = ['avg_speed']
        print("Task #11 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_12(self):
        """Найдите среднюю скорость ПК-блокнотов, цена которых превышает 1000 дол."""
        query = self.session.execute(
            select(
                func.avg(Laptop.speed)
            )
            .filter(Laptop.price > 1000)
        )

        headers = ['avg_speed']
        print("Task #12 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_13(self):
        """Найдите среднюю скорость ПК, выпущенных производителем A."""
        query = self.session.execute(
            select(
                func.avg(PC.speed)
            )
            .join(Product, PC.model_id == Product.model)
            .filter(Product.maker == 'A')
        )

        headers = ['avg_speed']
        print("Task #13 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_15(self):
        """Найдите размеры жестких дисков, совпадающих у двух и более PC. Вывести: HD"""
        query = self.session.execute(
            select(
                PC.hd
            )
            .group_by(PC.hd)
            .having(func.count(PC.hd) >= 2)
            .order_by(PC.hd)
        )

        headers = ['hd']
        print("Task #15 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_16(self):
        """
        Найдите пары моделей PC, имеющих одинаковые скорость и RAM.
        В результате каждая пара указывается только один раз, т.е. (i,j), но не (j,i),
        Порядок вывода: модель с большим номером, модель с меньшим номером, скорость и RAM.
        """
        a = aliased(PC)
        b = aliased(PC)

        query = self.session.execute(
            select(
                distinct(a.model_id),
                b.model_id,
                a.speed,
                a.ram
            )
            .filter(and_(a.model_id > b.model_id, a.speed == b.speed, a.ram == b.ram))
        )

        headers = ['A_model', 'B_model', 'speed', 'ram']
        print("Task #16 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_17(self):
        """
        Найдите модели ПК-блокнотов, скорость которых меньше скорости каждого из ПК.
        Вывести: type, model, speed"""
        query = self.session.execute(
            select(
                distinct(Product.type),
                Laptop.model_id,
                Laptop.speed
            )
            .join(Laptop, Product.model == Laptop.model_id)
            .filter(Laptop.speed < all_((select(PC.speed)).scalar_subquery()))
        )

        headers = ['type', 'model', 'speed']
        print("Task #17 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_18(self):
        """
        Найдите производителей самых дешевых цветных принтеров.
        Вывести: maker, price
        """
        subquery_printer_min_price = \
            select(
                func.min(Printer.price)
            ). \
                filter(Printer.color == 'y'). \
                scalar_subquery()

        query = self.session.execute(
            select(
                distinct(Product.maker),
                Printer.price
            )
            .join(Product, Printer.model_id == Product.model)
            .filter(and_(Printer.price == subquery_printer_min_price, Printer.color == 'y'))
        ).all()

        headers = ['maker', 'price']
        print("Task #18 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_19(self):
        """
        Для каждого производителя, имеющего модели в таблице Laptop,
        найдите средний размер экрана выпускаемых им ПК-блокнотов.
        Вывести: maker, средний размер экрана.
        """
        query = self.session.execute(
            select(
                Product.maker,
                func.avg(Laptop.screen)
            )
            .join(Laptop, Product.model == Laptop.model_id)
            .filter(Product.type == 'Laptop')
            .group_by(Product.maker)
        )

        headers = ['maker', 'avg_screen']
        print("Task #19 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_20(self):
        """
        Найдите производителей, выпускающих по меньшей мере три различных модели ПК.
        Вывести: Maker, число моделей ПК.
        """
        query = self.session.execute(
            select(
                Product.maker,
                func.count(Product.model)
            )
            .filter(Product.type == 'PC')
            .group_by(Product.maker)
            .having(func.count(Product.model) >= 3)
        )

        headers = ['maker', 'model_count']
        print("Task #20 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_21(self):
        """
        Найдите максимальную цену ПК, выпускаемых каждым производителем,
        у которого есть модели в таблице PC.
        Вывести: maker, максимальная цена.
        """
        query = self.session.execute(
            select(
                Product.maker,
                func.max(PC.price)
            )
            .join(PC, PC.model_id == Product.model)
            .filter(Product.type == 'PC')
            .group_by(Product.maker)
        )

        headers = ['maker', 'pc_max_price']
        print("Task #21 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_22(self):
        """
        Для каждого значения скорости ПК, превышающего 600 МГц,
        определите среднюю цену ПК с такой же скоростью.
        Вывести: speed, средняя цена.
        """
        query = self.session.execute(
            select(
                PC.speed,
                func.avg(PC.price)
            )
            .filter(PC.speed > 600)
            .group_by(PC.speed)
        )

        headers = ['speed', 'pc_avg_price']
        print("Task #22 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_23(self):
        """
        Найдите производителей, которые производили бы как ПК
        со скоростью не менее 750 МГц, так и ПК-блокноты со скоростью не менее 750 МГц.
        Вывести: Maker
        """
        query = self.session.execute(
            intersect(
                select(
                    Product.maker
                )
                .join(PC, Product.model == PC.model_id)
                .filter(PC.speed >= 750),

                select(
                    Product.maker
                )
                .join(Laptop, Laptop.model_id == Product.model)
                .filter(Laptop.speed >= 750)
            )
        )

        headers = ['maker']
        print("Task #23 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_24(self):
        """
        Перечислите номера моделей любых типов,
        имеющих самую высокую цену по всей имеющейся в базе данных продукции.
        """

        max_price = cte(
            union(
                select(PC.model_id, PC.price)
                .filter(PC.price == select(func.max(PC.price)).scalar_subquery()),

                select(Laptop.model_id, Laptop.price)
                .filter(Laptop.price == select(func.max(Laptop.price)).scalar_subquery()),

                select(Printer.model_id, Printer.price)
                .filter(Printer.price == select(func.max(Printer.price)).scalar_subquery())
            )
        )

        query = self.session.execute(
            select(max_price.c.model_id)
            .filter(max_price.c.price ==
                    select(func.max(max_price.c.price)).scalar_subquery())
        )

        headers = ['model']
        print("Task #24 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_25(self):
        """
        Найдите производителей принтеров, которые производят ПК с
        наименьшим объемом RAM и с самым быстрым процессором среди всех ПК,
        имеющих наименьший объем RAM.
        Вывести: Maker
        """
        min_ram_sub = (
            select(func.min(PC.ram)).scalar_subquery()
        )

        pc_spec = cte(
            select(
                func.max(PC.speed).label('max_speed'),
                PC.ram
            )
            .filter(PC.ram.in_(min_ram_sub))
            .group_by(PC.ram)
        )

        maker_table = (
            select(Product.maker)
            .join(PC, PC.model_id == Product.model)
            .join(pc_spec, (pc_spec.c.max_speed == PC.speed) & (pc_spec.c.ram == PC.ram))
        )

        query = self.session.execute(
            select(
                distinct(Product.maker)
            )
            .filter((Product.type == 'Printer') & Product.maker.in_(maker_table))
        )

        headers = ['maker']
        print("Task #25 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))


if __name__ == '__main__':
    # Сессия будет автоматически закрыта после выхода из блока with
    with ComputerFirmTasks() as comp_firm_task:
        # Выполняем задачи
        comp_firm_task.task_25()
