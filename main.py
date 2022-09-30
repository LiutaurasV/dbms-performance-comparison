from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from utils import timeit

conn_url = "postgresql+psycopg2://postgres:postgres@0.0.0.0:8432/postgres"
engine = create_engine(conn_url)

Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()


class Ship(Base):
    __tablename__ = "ship"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    weight = Column(Integer)


@timeit
def create_new_row(model, **kwargs):
    new_row = model(**kwargs)
    session.add(new_row)
    session.commit()


create_new_row(Ship, name="Black Betty", weight=14000)
