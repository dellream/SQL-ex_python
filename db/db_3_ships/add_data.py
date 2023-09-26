from datetime import datetime
from db.database import get_session
from db.db_3_ships.models import Classes, Ships, Outcomes, Battles


def add_data_to_ships():
    # Получаем сессию
    session = get_session()

    # Добавляем данные в таблицу Classes
    classes_entries = [
        Classes(class_name="Bismarck", type="bb", country="Germany", numGuns=8, bore=15.0, displacement=42000),
        Classes(class_name="Iowa", type="bb", country="USA", numGuns=9, bore=16.0, displacement=46000),
        Classes(class_name="Kongo", type="bc", country="Japan", numGuns=8, bore=14.0, displacement=32000),
        Classes(class_name="North Carolina", type="bb", country="USA", numGuns=12, bore=16.0, displacement=37000),
        Classes(class_name="Renown", type="bc", country="Gt.Britain", numGuns=6, bore=15.0, displacement=32000),
        Classes(class_name="Revenge", type="bb", country="Gt.Britain", numGuns=8, bore=15.0, displacement=29000),
        Classes(class_name="Tennessee", type="bb", country="USA", numGuns=12, bore=14.0, displacement=32000),
        Classes(class_name="Yamato", type="bb", country="Japan", numGuns=9, bore=18.0, displacement=65000)
    ]

    session.add_all(classes_entries)

    # Добавляем данные в таблицу Ships
    ships_entries = [
        Ships(name="California", class_name="Tennessee", launched=1921),
        Ships(name="Haruna", class_name="Kongo", launched=1916),
        Ships(name="Hiei", class_name="Kongo", launched=1914),
        Ships(name="Iowa", class_name="Iowa", launched=1943),
        Ships(name="Kirishima", class_name="Kongo", launched=1915),
        Ships(name="Kongo", class_name="Kongo", launched=1913),
        Ships(name="Missouri", class_name="Iowa", launched=1944),
        Ships(name="Musashi", class_name="Yamato", launched=1942),
        Ships(name="New Jersey", class_name="Iowa", launched=1943),
        Ships(name="North Carolina", class_name="North Carolina", launched=1941),
        Ships(name="Ramillies", class_name="Revenge", launched=1917),
        Ships(name="Renown", class_name="Renown", launched=1916),
        Ships(name="Repulse", class_name="Renown", launched=1916),
        Ships(name="Resolution", class_name="Renown", launched=1916),
        Ships(name="Revenge", class_name="Revenge", launched=1916),
        Ships(name="Royal Oak", class_name="Revenge", launched=1916),
        Ships(name="Royal Sovereign", class_name="Revenge", launched=1916),
        Ships(name="South Dakota", class_name="North Carolina", launched=1941),
        Ships(name="Tennessee", class_name="Tennessee", launched=1920),
        Ships(name="Washington", class_name="North Carolina", launched=1941),
        Ships(name="Wisconsin", class_name="Iowa", launched=1944),
        Ships(name="Yamato", class_name="Yamato", launched=1941)
    ]

    session.add_all(ships_entries)

    # Добавляем данные в таблицу Outcomes
    outcomes_entries = [
        Outcomes(ship="Bismarck", battle="North Atlantic", result="sunk"),
        Outcomes(ship="California", battle="Guadalcanal", result="damaged"),
        Outcomes(ship="California", battle="Surigao Strait", result="ok"),
        Outcomes(ship="Duke of York", battle="North Cape", result="ok"),
        Outcomes(ship="Fuso", battle="Surigao Strait", result="sunk"),
        Outcomes(ship="Hood", battle="North Atlantic", result="sunk"),
        Outcomes(ship="King George V", battle="North Atlantic", result="ok"),
        Outcomes(ship="Kirishima", battle="Guadalcanal", result="sunk"),
        Outcomes(ship="Prince of Wales", battle="North Atlantic", result="damaged"),
        Outcomes(ship="Rodney", battle="North Atlantic", result="OK"),
        Outcomes(ship="Schamhorst", battle="North Cape", result="sunk"),
        Outcomes(ship="South Dakota", battle="Guadalcanal", result="damaged"),
        Outcomes(ship="Tennessee", battle="Surigao Strait", result="ok"),
        Outcomes(ship="Washington", battle="Guadalcanal", result="ok"),
        Outcomes(ship="West Virginia", battle="Surigao Strait", result="ok"),
        Outcomes(ship="Yamashiro", battle="Surigao Strait", result="sunk")
    ]

    session.add_all(outcomes_entries)

    # Добавляем данные в таблицу Battles
    battles_entries = [
        Battles(name="#Cuba62a", date=datetime(1962, 10, 20)),
        Battles(name="#Cuba62b", date=datetime(1962, 10, 25)),
        Battles(name="Guadalcanal", date=datetime(1942, 11, 15)),
        Battles(name="North Atlantic", date=datetime(1941, 5, 25)),
        Battles(name="North Cape", date=datetime(1943, 12, 26)),
        Battles(name="Surigao Strait", date=datetime(1944, 10, 25))
    ]

    session.add_all(battles_entries)

    # Завершаем транзакцию и сохраняем изменения в БД
    session.commit()


if __name__ == '__main__':
    add_data_to_ships()
