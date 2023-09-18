from sqlalchemy import Column, Integer, String, Float, Enum, ForeignKey
from sqlalchemy.orm import relationship, declarative_base

from db.database import engine

Base = declarative_base()


class Product(Base):
    __tablename__ = 'product'

    model = Column(Integer, primary_key=True)
    maker = Column(String, nullable=False)
    type = Column(Enum('PC', 'Laptop', 'Printer', name='product_type'), nullable=False)

    # Определяем отношение к таблице PC
    pcs = relationship('PC', back_populates='product')
    laptops = relationship('Laptop', back_populates='product')
    printers = relationship('Printer', back_populates='product')


class PC(Base):
    __tablename__ = 'pc'

    code = Column(Integer, primary_key=True)
    model_id = Column(Integer, ForeignKey('product.model'))
    speed = Column(Integer)
    ram = Column(Integer)
    hd = Column(Float)
    cd = Column(String)
    price = Column(Float)

    # Определяем обратное отношение к таблице Product
    product = relationship('Product', back_populates='pcs')


class Laptop(Base):
    __tablename__ = 'laptop'

    code = Column(Integer, primary_key=True)
    model_id = Column(Integer, ForeignKey('product.model'))
    speed = Column(Integer)
    ram = Column(Integer)
    hd = Column(Float)
    price = Column(Float)
    screen = Column(Integer)

    # Определяем обратное отношение к таблице Product
    product = relationship('Product', back_populates='laptops')


class Printer(Base):
    __tablename__ = 'printer'

    code = Column(Integer, primary_key=True)
    model_id = Column(Integer, ForeignKey('product.model'))
    color = Column(String)
    type = Column(String)
    price = Column(Float)

    # Определяем обратное отношение к таблице Product
    product = relationship('Product', back_populates='printers')


if __name__ == "__main__":
    # Создаем БД:
    Base.metadata.create_all(engine)
