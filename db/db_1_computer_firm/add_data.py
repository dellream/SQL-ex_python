from db.database import get_session
from db.db_1_computer_firm.models import Product, PC, Laptop, Printer, engine

# Получаем сессию
session = get_session()

# Очищаем таблицы перед вставкой данных
session.query(Product).delete()
session.query(PC).delete()
session.query(Laptop).delete()
session.query(Printer).delete()

# Вставляем данные в таблицу Product
products = [
    Product(model=1121, maker='B', type='PC'),
    Product(model=1232, maker='A', type='PC'),
    Product(model=1233, maker='A', type='PC'),
    Product(model=1260, maker='E', type='PC'),
    Product(model=1276, maker='A', type='Printer'),
    Product(model=1288, maker='D', type='Printer'),
    Product(model=1298, maker='A', type='Laptop'),
    Product(model=1321, maker='C', type='Laptop'),
    Product(model=1401, maker='A', type='Printer'),
    Product(model=1408, maker='A', type='Printer'),
    Product(model=1433, maker='D', type='Printer'),
    Product(model=1434, maker='E', type='Printer'),
    Product(model=1750, maker='B', type='Laptop'),
    Product(model=1752, maker='A', type='Laptop'),
    Product(model=2112, maker='E', type='PC'),
    Product(model=2113, maker='E', type='PC')
]

session.add_all(products)

# Вставляем данные в таблицу PC
pcs = [
    PC(model_id=1121, speed=750, ram=128, hd=14.0, cd='40x', price=850.0),
    PC(model_id=1232, speed=500, ram=64, hd=5.0, cd='12x', price=600.0),
    PC(model_id=1233, speed=500, ram=64, hd=5.0, cd='12x', price=600.0),
    PC(model_id=1121, speed=600, ram=128, hd=14.0, cd='40x', price=850.0),
    PC(model_id=1121, speed=600, ram=128, hd=8.0, cd='40x', price=850.0),
    PC(model_id=1233, speed=750, ram=128, hd=20.0, cd='50x', price=950.0),
    PC(model_id=1232, speed=500, ram=32, hd=10.0, cd='12x', price=400.0),
    PC(model_id=1232, speed=450, ram=64, hd=8.0, cd='24x', price=350.0),
    PC(model_id=1232, speed=450, ram=32, hd=10.0, cd='24x', price=350.0),
    PC(model_id=1260, speed=500, ram=32, hd=10.0, cd='12x', price=350.0),
    PC(model_id=1233, speed=900, ram=128, hd=40.0, cd='40x', price=980.0),
    PC(model_id=1233, speed=800, ram=128, hd=20.0, cd='50x', price=970.0)
]

session.add_all(pcs)

# Вставляем данные в таблицу Laptop
laptops = [
    Laptop(model_id=1298, speed=350, ram=32, hd=4.0, price=700.0, screen=11),
    Laptop(model_id=1321, speed=500, ram=64, hd=8.0, price=970.0, screen=12),
    Laptop(model_id=1750, speed=750, ram=128, hd=12.0, price=1200.0, screen=14),
    Laptop(model_id=1298, speed=600, ram=64, hd=10.0, price=1050.0, screen=15),
    Laptop(model_id=1752, speed=750, ram=128, hd=10.0, price=1150.0, screen=14),
    Laptop(model_id=1298, speed=450, ram=64, hd=10.0, price=950.0, screen=12)
]

session.add_all(laptops)

# Вставляем данные в таблицу Printer
printers = [
    Printer(model_id=1288, color='n', type='Laser', price=400.0),
    Printer(model_id=1408, color='n', type='Matrix', price=270.0),
    Printer(model_id=1401, color='n', type='Matrix', price=150.0),
    Printer(model_id=1434, color='y', type='Jet', price=290.0),
    Printer(model_id=1433, color='y', type='Jet', price=270.0),
    Printer(model_id=1276, color='n', type='Laser', price=400.0)
]

session.add_all(printers)

# Завершаем транзакцию и сохраняем изменения в БД
session.commit()

# Закрываем сессию
session.close()
