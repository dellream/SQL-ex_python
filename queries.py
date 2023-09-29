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
    cte,
    case,
    literal_column,
    Float,
    String,
)
from sqlalchemy.orm import aliased
from sqlalchemy.sql.expression import case
from tabulate import tabulate

from db.database import get_session, exec_query
from db.db_1_computer_firm.models import Product, PC, Laptop, Printer
from db.db_2_recycling_firm.models import Income, Outcome, Income_o, Outcome_o
from db.db_3_ships.models import Outcomes, Ships, Classes


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
        subquery_printer_min_price = (
            select(
                func.min(Printer.price)
            )
            .filter(Printer.color == 'y')
            .scalar_subquery()
        )

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

        with pc_spec AS (
        SELECT max(speed) max_speed, ram
        FROM pc
        WHERE ram IN (select min(ram) min_ram from pc)
        GROUP BY ram)

        SELECT DISTINCT maker
        FROM product
        WHERE type = 'Printer'
            AND maker IN (SELECT maker
                            FROM product
                            JOIN pc ON pc.model = product.model
                            JOIN pc_spec ON pc_spec.max_speed = pc.speed
                                AND pc_spec.ram = pc.ram)
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

    def task_26(self):
        """
        Найдите среднюю цену ПК и ПК-блокнотов, выпущенных производителем A.
        Вывести: одна общая средняя цена.

        WITH tmp AS (
        SELECT model, price
        FROM PC
        UNION ALL
        SELECT model, price
        FROM Laptop)

        SELECT avg(price) as AVG_price
        FROM tmp JOIN product
        ON product.model = tmp.model
        WHERE maker = 'A'
        """
        tmp = cte(
            union_all(
                select(PC.model_id, PC.price),
                select(Laptop.model_id, Laptop.price)
            )
        )

        query = self.session.execute(
            select(
                func.avg(tmp.c.price)
            )
            .join(Product, Product.model == tmp.c.model_id)
            .filter(Product.maker == 'A')
        )

        headers = ['AVG_price']
        print("Task #26 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_27(self):
        """
        Найдите средний размер диска ПК каждого из тех производителей,
        которые выпускают и принтеры.
        Вывести: maker, средний размер HD.
        """
        subquery = (
            select(Product.maker)
            .filter(Product.type == 'Printer')
        )

        query = self.session.execute(
            select(
                distinct(Product.maker),
                func.avg(PC.hd)
            )
            .join(PC, PC.model_id == Product.model)
            .filter(Product.maker.in_(subquery))
            .group_by(Product.maker)
        )

        headers = ['maker', 'AVG_hd']
        print("Task #27 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_28(self):
        """
        Используя таблицу Product, определить количество производителей,
        выпускающих по одной модели.
        """
        tmp_cte = cte(
            select(Product.maker)
            .group_by(Product.maker)
            .having(func.count(Product.model) == 1)
        )

        query = self.session.execute(
            select(func.count(tmp_cte.c.maker))
        )

        headers = ['Qty']
        print("Task #28 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_35(self):
        """
        В таблице Product найти модели, которые состоят только из цифр
        или только из латинских букв (A-Z, без учета регистра).
        Вывод: номер модели, тип модели.

        SELECT model, type
        FROM Product
        WHERE model NOT LIKE '%[^0-9]%'
          OR model NOT LIKE '%[^A-Z]%';
        """
        query = self.session.execute(
            select(
                Product.model,
                Product.type
            )
            .filter(
                or_(
                    func.cast(Product.model, String).notlike('%[^0-9]%'),
                    func.cast(Product.model, String).notlike('%[^A-Z]%')
                )
            )
        )

        headers = ['Model', 'Type']
        print("Task #35 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_75(self):
        """
        Для тех производителей, у которых есть продукты с известной ценой хотя бы в одной
        из таблиц Laptop, PC, Printer найти максимальные цены на каждый из типов продукции.
        Вывод: maker, максимальная цена на ноутбуки, максимальная цена на ПК, максимальная цена на принтеры.
        Для отсутствующих продуктов/цен использовать NULL.

        SELECT
          maker,
          [laptop],
          [pc],
          [printer]
        FROM (SELECT
                maker,
                TYPE,
                MAX (price)(price) price
               FROM Product p JOIN (SELECT model, price
                                    FROM PC
                                    UNION
                                    SELECT model, price
                                    FROM Laptop
                                    UNION
                                    SELECT model, price
                                    FROM Printer) as W
                ON p.model = w.model
                GROUP BY maker, TYPE
                HAVING MAX (price) IS NOT NULL
        ) A PIVOT (
                    MAX (price) FOR TYPE IN ([laptop], [pc], [printer])
                  ) pvt;
        """

    def task_82(self):
        """
        В наборе записей из таблицы PC, отсортированном по столбцу code (по возрастанию)
        найти среднее значение цены для каждой шестерки подряд идущих ПК.
        Вывод: значение code, которое является первым в наборе из шести строк, среднее значение цены в наборе.

        WITH CTE(code, price, number) AS (
            SELECT
                PC.code,
                PC.price,
                number = ROW_NUMBER() OVER (ORDER BY PC.code)
            FROM PC
        )
        SELECT
            CTE.code,
            AVG(C.price)
        FROM CTE JOIN CTE C
        ON (C.number - CTE.number) < 6 AND (C.number - CTE.number) >= 0
        GROUP BY CTE.number, CTE.code
        HAVING COUNT(CTE.number) = 6;
        """

    def task_89(self):
        """
        Найти производителей, у которых больше всего моделей в таблице Product,
        а также тех, у которых меньше всего моделей.
        Вывод: maker, число моделей

        WITH a AS (
            SELECT
                maker,
                count(*) AS cnt,
                max(count(model)) over (PARTITION by 1) AS maxcol,
                min(count(model)) over (PARTITION by 1) AS mincol
            FROM Product
            GROUP BY maker
        )
        SELECT
            maker,
            cnt
        FROM a
        WHERE
            cnt = maxcol
            OR cnt = mincol;
        """

class RecyclingFirmTasks(SessionCreater):
    """
    Класс для решения задач по второй БД (Фирма вторсырья)
    """

    def task_29(self):
        """
        В предположении, что приход и расход денег на каждом пункте приема
        фиксируется не чаще одного раза в день [т.е. первичный ключ (пункт, дата)],
        написать запрос с выходными данными (пункт, дата, приход, расход).

        Использовать таблицы Income_o и Outcome_o.

        SELECT
            CASE
                WHEN i.point IS NOT NULL THEN i.point
                ELSE o.point
            END,
            CASE
                WHEN i.date IS NOT NULL THEN i.date
                ELSE o.date
            END,
            inc,
            out
        FROM Income_o i
        FULL OUTER JOIN Outcome_o o
            ON i.point = o.point
        AND i.date = o.date;
        """
        query = self.session.execute(
            select(
                case((Income_o.point.isnot(None), Income_o.point),
                     else_=Outcome_o.point),
                case((Income_o.date.isnot(None), Income_o.date),
                     else_=Outcome_o.date),
                Income_o.inc,
                Outcome_o.out
            )
            .join(Income_o,
                  (Income_o.point == Outcome_o.point) & (Income_o.date == Outcome_o.date),
                  full=True)
        )

        headers = ['POINT', 'DATE', 'inc', 'out']
        print("Task #29 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_30(self):
        """
        В предположении, что приход и расход денег на каждом
        пункте приема фиксируется произвольное число раз (первичным ключом
        в таблицах является столбец code), требуется получить таблицу,
        в которой каждому пункту за каждую дату выполнения операций будет
        соответствовать одна строка.

        SELECT
          point,
          date,
          sum(sum_out) as sum_out,
          sum(sum_inc) as sum_inc
        FROM (SELECT point,
                     date,
                     sum(inc) as sum_inc,
                     NULL as sum_out
              FROM income
              GROUP BY point, date
              UNION
              SELECT point,
                     date,
                     NULL sum_inc,
                     sum(out) as sum_out
              FROM outcome
              GROUP BY point, date) t
        GROUP BY point, date
        """
        stmt = union(
            select(
                Income.point,
                Income.date,
                literal_column("NULL").label('sum_out'),
                func.sum(Income.inc).label('sum_inc')
            )
            .group_by(Income.point, Income.date),

            select(
                Outcome.point,
                Outcome.date,
                func.sum(Outcome.out).label('sum_out'),
                literal_column("NULL").label('sum_inc')
            )
            .group_by(Outcome.point, Outcome.date)
        )

        subq = stmt.alias('subq')

        query = self.session.execute(
            select(
                subq.c.point,
                subq.c.date,
                func.sum(subq.c.sum_out).label('sum_out'),
                func.sum(subq.c.sum_inc).label('sum_inc')
            )
            .group_by(subq.c.point, subq.c.date)
            .order_by(subq.c.date)
        )

        headers = ['POINT', 'DATE', 'sum_out', 'sum_inc']
        print("Task #30 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))


class ShipsTasks(SessionCreater):
    def task_31(self):
        """
        SELECT class, country
        FROM Classes
        WHERE bore >= 16
        """
        query = self.session.execute(
            select(
                Classes.class_name,
                Classes.country
            )
            .filter(Classes.bore >= 16)
        )

        headers = ['Class_name', 'Country']
        print("Task #30 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_32(self):
        """
        Одной из характеристик корабля является половина куба калибра его главных орудий (mw).
        С точностью до 2 десятичных знаков определите среднее значение mw для кораблей каждой
        страны, у которой есть корабли в базе данных.

        SELECT
            country,
            CAST(AVG(POWER(bore, 3) / 2) AS numeric(6, 2)) AS weight
        FROM Classes
         JOIN (SELECT class
               FROM Ships
               UNION ALL
              (SELECT ship AS class
               FROM Outcomes
               EXCEPT
               SELECT
               name AS class
               FROM
               Ships)) AS t --Исключаем корабли, которые присутствуют и в таблице Outcomes, и в таблице Ships
          ON Classes.class = t.class
        GROUP BY country
        """
        # Исключаем корабли, которые присутствуют и в таблице Outcomes, и в таблице Ships
        except_subquery = except_(
            select(Outcomes.ship.label('class_name')),
            select(Ships.name.label('class_name'))
        )

        # Объединяем подзапросы для join
        union_subqueries = union_all(select(Ships.class_name), except_subquery).alias()

        query = self.session.execute(
            select(
                Classes.country,
                func.cast(func.avg(func.power(Classes.bore, 3) / 2), Float(6, 2)).label('weight')
            )
            .join(
                union_subqueries,
                Classes.class_name == union_subqueries.c.class_name
            )
            .group_by(Classes.country)
        )

        headers = ['Country', 'Weight']
        print("Task #32 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_33(self):
        """
        Укажите корабли, потопленные в сражениях в Северной Атлантике (North Atlantic). Вывод: ship.

        SELECT ship
        FROM Outcomes
        WHERE battle LIKE '%Atlantic'
          AND result = 'sunk'
        """
        query = self.session.execute(
            select(
                Outcomes.ship
            )
            .filter(and_(Outcomes.battle.like('%Atlantic'), Outcomes.result == 'sunk'))
        )

        headers = ['ships_name']
        print("Task #33 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_34(self):
        """
        По Вашингтонскому международному договору от начала 1922 г. запрещалось
        строить линейные корабли водоизмещением более 35 тыс. тонн. Укажите корабли,
        нарушившие этот договор (учитывать только корабли c известным годом спуска на воду).
        Вывести названия кораблей.

        SELECT
          s.name
        FROM classes c JOIN ships s
          ON c.class = s.class
        WHERE s.launched >= 1922
          AND c.type = 'bb'
          AND c.displacement > 35000
        """
        query = self.session.execute(
            select(
                Ships.name
            )
            .join(Classes, Classes.class_name == Ships.class_name)
            .filter(
                and_(
                    Ships.launched >= 1922,
                    Classes.type == 'bb',
                    Classes.displacement > 35000
                )
            )
        )

        headers = ['ships_name']
        print("Task #34 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_36(self):
        """
        Перечислите названия головных кораблей,
        имеющихся в базе данных (учесть корабли в Outcomes).

        SELECT name
        FROM Ships
        WHERE name = class
        UNION
        SELECT ship
        FROM Outcomes, Classes
        WHERE class = ship
        """

        query = self.session.execute(
            union(
                select(Ships.name).filter(Ships.name == Ships.class_name),
                select(Outcomes.ship).join(Classes, Classes.class_name == Outcomes.ship)
            )
        )

        headers = ['ships_name']
        print("Task #36 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_37(self):
        """
        Найдите классы, в которые входит только один
        корабль из базы данных (учесть также корабли в Outcomes).

        SELECT class
        FROM (
          SELECT name, class
          FROM Ships
          UNION
          SELECT class AS name, class
          FROM Classes JOIN Outcomes
            ON Classes.class = Outcomes.ship
        ) tmp
        GROUP BY class
        HAVING count(tmp.name) = 1
        """

        from_subquery = (
            union(
                select(Ships.name, Ships.class_name),
                select(
                    Classes.class_name.label('name'),
                    Classes.class_name
                ).join(Outcomes, Classes.class_name == Outcomes.ship)
            )
        ).alias()

        query = self.session.execute(
            select(
                from_subquery.c.class_name
            )
            .group_by(from_subquery.c.class_name)
            .having(func.count(from_subquery.c.name) == 1)
        )

        headers = ['class_name']
        print("Task #37 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_38(self):
        """
        Найдите страны, имевшие когда-либо классы обычных боевых кораблей ('bb')
        и имевшие когда-либо классы крейсеров ('bc').

        SELECT country
        FROM Classes
        WHERE type = 'bb'
        INTERSECT
        SELECT country
        FROM Classes
        WHERE type = 'bc'
        """
        query = self.session.execute(
            intersect(
                select(Classes.country).filter(Classes.type == 'bb'),
                select(Classes.country).filter(Classes.type == 'bc')
            )
        )

        headers = ['country']
        print("Task #38 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_52(self):
        """
        Определить названия всех кораблей из таблицы Ships,
        которые могут быть линейным японским кораблем, имеющим
        число главных орудий не менее девяти, калибр орудий менее 19 дюймов
        и водоизмещение не более 65 тыс. тонн

        SELECT name
        FROM ships JOIN classes
          ON classes.class = ships.class
        WHERE
            COALESCE(country, 'Japan') = 'Japan'
            AND COALESCE(numGuns, 9) >= 9
            AND COALESCE(displacement, 65000) <= 65000
            AND COALESCE(TYPE, 'bb') = 'bb'
            AND COALESCE(bore, 18) < 19
        """
        query = self.session.execute(
            select(
                Ships.name
            )
            .join(Classes, Classes.class_name == Ships.class_name)
            .filter(
                func.coalesce(Classes.country, 'Japan') == 'Japan',
                func.coalesce(Classes.numGuns, '9') >= '9',
                func.coalesce(Classes.displacement, '65000') <= '65000',
                func.coalesce(Classes.type, 'bb') == 'bb',
                func.coalesce(Classes.bore, '18') < '19'
            )
        )

        headers = ['ships_name']
        print("Task #52 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))

    def task_56(self):
        """
        Для каждого класса определите число кораблей этого класса, потопленных в сражениях.
        Вывести: класс и число потопленных кораблей.

        with tmp as (
         -- Таблица результатов боев головных кораблей из outcomes и неголовных из Classes
         SELECT c.class, o.result, o.ship
         FROM Classes c LEFT JOIN Outcomes o
          ON c.class = o.ship
          UNION
         -- Таблица результатов боев не головных кораблей в т.ч из outcomes
         SELECT s.class, o.result, s.name
         FROM Outcomes o JOIN Ships s
          ON o.ship = s.name
        )

        SELECT
          class,
          sum (
               CASE
                WHEN result = 'sunk' THEN 1
                ELSE 0
               END
              ) as sunks_qty
        FROM tmp
        GROUP BY class
        """
        union_1 = (
            select(
                Classes.class_name,
                Outcomes.result,
                Outcomes.ship.label('name')
            )
            .join(Outcomes, Outcomes.ship == Classes.class_name, isouter=True)
        )

        union_2 = (
            select(
                Ships.class_name,
                Outcomes.result,
                Ships.name
            )
            .join(Outcomes, Ships.name == Outcomes.ship)
        )

        tmp_cte = union(union_1, union_2).cte('tmp_cte')

        final_query = (
            select(
                tmp_cte.c.class_name,
                func.sum(
                    case(
                        (tmp_cte.c.result == 'sunk', 1), else_=0
                    )
                ).label('sunks_qty')
            )
            .group_by(tmp_cte.c.class_name)
        )

        query = self.session.execute(final_query)

        headers = ['class_name', 'sunks_qty']
        print("Task #56 (SQL-Alchemy):")
        print(tabulate(query, headers, tablefmt='pretty'))


if __name__ == '__main__':
    # Сессия будет автоматически закрыта после выхода из блока with

    # with ComputerFirmTasks() as comp_firm_task:
    #     comp_firm_task.task_35()

    # with RecyclingFirmTasks() as recycling_firm_task:
    #     # Выполняем задачи
    #     recycling_firm_task.task_30()

    with ShipsTasks() as ships_task:
        # Выполняем задачи
        ships_task.task_56()
