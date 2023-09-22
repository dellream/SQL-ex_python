from sqlalchemy import Column, Integer, Float, Date
from sqlalchemy.orm import declarative_base

from db.database import engine

Base = declarative_base()


class Income(Base):
    __tablename__ = 'income'
    code = Column(Integer, primary_key=True)
    point = Column(Integer)
    date = Column(Date)
    inc = Column(Float)


class Income_o(Base):
    __tablename__ = 'income_o'
    point = Column(Integer, primary_key=True)
    date = Column(Date, primary_key=True)
    inc = Column(Float)


class Outcome(Base):
    __tablename__ = 'outcome'
    code = Column(Integer, primary_key=True)
    point = Column(Integer)
    date = Column(Date)
    out = Column(Float)


class Outcome_o(Base):
    __tablename__ = 'outcome_o'
    point = Column(Integer, primary_key=True)
    date = Column(Date, primary_key=True)
    out = Column(Float)


if __name__ == "__main__":
    # Создаем БД:
    Base.metadata.create_all(engine)
