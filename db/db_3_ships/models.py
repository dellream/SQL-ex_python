from sqlalchemy import Column, Integer, Float, Date, String, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Classes(Base):
    __tablename__ = 'classes'
    class_name = Column(String, primary_key=True, nullable=False)
    type = Column(String, nullable=False)
    country = Column(String, nullable=False)
    numGuns = Column(Integer)
    bore = Column(Float)
    displacement = Column(Integer)

    ships = relationship('Ships', back_populates='classes')


class Ships(Base):
    __tablename__ = 'ships'
    name = Column(String, primary_key=True, nullable=False)
    class_name = Column(String, ForeignKey('classes.class_name'), nullable=False)
    launched = Column(Integer)

    classes = relationship('Classes', back_populates='ships')


class Outcomes(Base):
    __tablename__ = 'outcomes'
    ship = Column(String, primary_key=True, nullable=False)
    battle = Column(String, ForeignKey('battles.name'), primary_key=True, nullable=False)
    result = Column(String, nullable=False)

    battles = relationship('Battles', back_populates='outcomes')


class Battles(Base):
    __tablename__ = 'battles'
    name = Column(String, primary_key=True, nullable=False)
    date = Column(Date, nullable=False)

    outcomes = relationship('Outcomes', back_populates='battles')


if __name__ == "__main__":
    from db.database import engine  # Замените на вашу настройку SQLAlchemy

    Base.metadata.create_all(engine)
