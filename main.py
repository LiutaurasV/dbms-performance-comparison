import csv
from datetime import datetime
from time import time

from sqlalchemy import Column, Integer, String, DateTime, Float, create_engine
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
    mmsi = Column(Integer, nullable=False)
    basedatetime = Column(DateTime, nullable=False)
    lat = Column(Float, nullable=False)
    long = Column(Float, nullable=False)
    sog = Column(Float, nullable=False)
    cog = Column(Float, nullable=False)
    heading = Column(Integer, nullable=False)
    vessel_name = Column(String, nullable=False)
    imo = Column(String)
    call_sign = Column(String)
    vessel_type = Column(Integer, nullable=False)
    status = Column(Integer)
    length = Column(Integer)
    width = Column(Integer)
    draft = Column(Float)
    cargo = Column(Integer)
    transceiver_class = Column(String, nullable=False)


try:
    Ship.__table__.drop(engine)
except:
    pass
Base.metadata.create_all(engine)


@timeit
def create_new_row(model, **kwargs):
    new_row = model(**kwargs)
    session.add(new_row)
    session.commit()


def add_new_ship(num_of_rows: int, file_path: str) -> None:
    with open(file_path) as data_file:
        csv_reader = csv.reader(data_file, delimiter=",")
        next(csv_reader)
        for _ in range(num_of_rows):
            row = next(csv_reader)
            ship_data = {
                "mmsi": row[0],
                "basedatetime": datetime.strptime(row[1], "%Y-%m-%dT%H:%M:%S"),
                "lat": float(row[2]),
                "long": float(row[3]),
                "sog": float(row[4]),
                "cog": float(row[5]),
                "heading": int(row[6]),
                "vessel_name": row[7],
                "vessel_type": int(row[10]),
                "transceiver_class": row[16],
            }
            ship_data["imo"] = row[8] if row[8] != "" else None
            ship_data["call_sign"] = row[9] if row[9] != "" else None
            ship_data["status"] = int(row[11]) if row[11] != "" else None
            ship_data["length"] = int(row[12]) if row[12] != "" else None
            ship_data["width"] = int(row[13]) if row[13] != "" else None
            ship_data["draft"] = float(row[14]) if row[14] != "" else None
            ship_data["cargo"] = int(row[15]) if row[15] != "" else None
            session.add(Ship(**ship_data))

    time_before = time()
    session.commit()
    return time() - time_before


# create_new_row(Ship, name="Black Betty", weight=14000)
