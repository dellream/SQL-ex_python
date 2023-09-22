from datetime import datetime
from db.database import get_session
from db.db_2_recycling_firm.models import Income, Outcome, Income_o, Outcome_o


def add_data_to_recycling_firm():
    # Получаем сессию
    session = get_session()

    incomes = [
        Income(code=1, point=1, date=datetime(2001, 3, 22), inc=15000.0000),
        Income(code=2, point=1, date=datetime(2001, 3, 23), inc=15000.0000),
        Income(code=3, point=1, date=datetime(2001, 3, 24), inc=3600.0000),
        Income(code=4, point=2, date=datetime(2001, 3, 22), inc=10000.0000),
        Income(code=5, point=2, date=datetime(2001, 3, 24), inc=1500.0000),
        Income(code=6, point=1, date=datetime(2001, 4, 13), inc=5000.0000),
        Income(code=7, point=1, date=datetime(2001, 5, 11), inc=4500.0000),
        Income(code=8, point=1, date=datetime(2001, 3, 22), inc=15000.0000),
        Income(code=9, point=2, date=datetime(2001, 3, 24), inc=1500.0000),
        Income(code=10, point=1, date=datetime(2001, 4, 13), inc=5000.0000),
        Income(code=11, point=1, date=datetime(2001, 3, 24), inc=3400.0000),
        Income(code=12, point=3, date=datetime(2001, 9, 13), inc=1350.0000),
        Income(code=13, point=3, date=datetime(2001, 9, 13), inc=1750.0000)
    ]

    session.add_all(incomes)

    # Добавляем данные в таблицу Income_o
    income_o_entries = [
        Income_o(point=1, date=datetime(2001, 3, 22), inc=15000.0000),
        Income_o(point=1, date=datetime(2001, 3, 23), inc=15000.0000),
        Income_o(point=1, date=datetime(2001, 3, 24), inc=3400.0000),
        Income_o(point=1, date=datetime(2001, 4, 13), inc=5000.0000),
        Income_o(point=1, date=datetime(2001, 5, 11), inc=4500.0000),
        Income_o(point=2, date=datetime(2001, 3, 22), inc=10000.0000),
        Income_o(point=2, date=datetime(2001, 3, 24), inc=1500.0000),
        Income_o(point=3, date=datetime(2001, 9, 13), inc=11500.0000),
        Income_o(point=3, date=datetime(2001, 10, 2), inc=18000.0000)
    ]

    session.add_all(income_o_entries)

    # Добавляем данные в таблицу Outcome
    outcomes = [
        Outcome(code=1, point=1, date=datetime(2001, 3, 14), out=15348.0000),
        Outcome(code=2, point=1, date=datetime(2001, 3, 24), out=3663.0000),
        Outcome(code=3, point=1, date=datetime(2001, 3, 26), out=1221.0000),
        Outcome(code=4, point=1, date=datetime(2001, 3, 28), out=2075.0000),
        Outcome(code=5, point=1, date=datetime(2001, 3, 29), out=2004.0000),
        Outcome(code=6, point=1, date=datetime(2001, 4, 11), out=3195.0400),
        Outcome(code=7, point=1, date=datetime(2001, 4, 13), out=4490.0000),
        Outcome(code=8, point=1, date=datetime(2001, 4, 27), out=3110.0000),
        Outcome(code=9, point=1, date=datetime(2001, 5, 11), out=2530.0000),
        Outcome(code=10, point=2, date=datetime(2001, 3, 22), out=1440.0000),
        Outcome(code=11, point=2, date=datetime(2001, 3, 29), out=7848.0000),
        Outcome(code=12, point=2, date=datetime(2001, 4, 2), out=2040.0000),
        Outcome(code=13, point=1, date=datetime(2001, 3, 24), out=3500.0000),
        Outcome(code=14, point=2, date=datetime(2001, 3, 22), out=1440.0000),
        Outcome(code=15, point=1, date=datetime(2001, 3, 29), out=2006.0000),
        Outcome(code=16, point=3, date=datetime(2001, 9, 13), out=1200.0000),
        Outcome(code=17, point=3, date=datetime(2001, 9, 13), out=1500.0000),
        Outcome(code=18, point=3, date=datetime(2001, 9, 14), out=1150.0000)
    ]

    session.add_all(outcomes)

    # Добавляем данные в таблицу Outcome_o
    outcome_o_entries = [
        Outcome_o(point=1, date=datetime(2001, 3, 14), out=15348.0000),
        Outcome_o(point=1, date=datetime(2001, 3, 24), out=3663.0000),
        Outcome_o(point=1, date=datetime(2001, 3, 26), out=1221.0000),
        Outcome_o(point=1, date=datetime(2001, 3, 28), out=2075.0000),
        Outcome_o(point=1, date=datetime(2001, 3, 29), out=2004.0000),
        Outcome_o(point=1, date=datetime(2001, 4, 11), out=3195.0400),
        Outcome_o(point=1, date=datetime(2001, 4, 13), out=4490.0000),
        Outcome_o(point=1, date=datetime(2001, 4, 27), out=3110.0000),
        Outcome_o(point=1, date=datetime(2001, 5, 11), out=2530.0000),
        Outcome_o(point=2, date=datetime(2001, 3, 22), out=1440.0000),
        Outcome_o(point=2, date=datetime(2001, 3, 29), out=7848.0000),
        Outcome_o(point=2, date=datetime(2001, 4, 2), out=2040.0000),
        Outcome_o(point=3, date=datetime(2001, 9, 13), out=1500.0000),
        Outcome_o(point=3, date=datetime(2001, 9, 14), out=2300.0000),
        Outcome_o(point=3, date=datetime(2002, 9, 16), out=2150.0000)
    ]

    session.add_all(outcome_o_entries)

    # Завершаем транзакцию и сохраняем изменения в БД
    session.commit()

    # Закрываем сессию
    session.close()


if __name__ == '__main__':
    add_data_to_recycling_firm()
