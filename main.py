import csv
import os
import uuid
import time

from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy import insert
from sqlalchemy import ForeignKey

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
# from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session


class Base(DeclarativeBase):
    pass

class Firstname(Base):
    __tablename__ = "unique_firstnames"

    uuid: Mapped[str] = mapped_column(primary_key=True)
    firstname: Mapped[str] = mapped_column(unique=True)
    gender: Mapped[str]

    # has a one-to-many relationship with class Position
    # positions: Mapped[list["Position"]] = relationship(back_populates="firstname")


class Position(Base):
    __tablename__ = "positions_firstname"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    firstname_uuid: Mapped[str] = mapped_column(ForeignKey("unique_firstnames.uuid"))
    position: Mapped[int]
    amount: Mapped[int]

    # has a many-to-one relationship with class Name
    # firstname: Mapped[str] = relationship(back_populates="positions")

engine = create_engine("sqlite:///test-all.db", echo=False)
Base.metadata.create_all(engine)


def parseFile(file: str) -> list:
    with open(file , newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        if len(reader.fieldnames) <= 1:
            csvfile.seek(0)
            reader = csv.DictReader(csvfile, delimiter=';')
        data = []
        for row in reader:
            if row['vorname'].__contains__('?'):
                continue
            if row['vorname'].__contains__('�'):
                continue
            # if row['vorname'].__contains__('-'):
            #     names = row['vorname'].split('-')
            #     for name in names:
            #         data.append({
            #             'vorname': name,
            #             'anzahl': int(row['anzahl']),
            #             'geschlecht': row['geschlecht'],
            #             'position': int(row['position'])
            #         })
            # else:
            data.append({
                'vorname': row['vorname'],
                'anzahl': int(row['anzahl']),
                'geschlecht': row['geschlecht'],
                'position': int(row['position'])
            })
    return data

def parseFilesInDirectory(directory: str) -> list:
    with os.scandir(directory) as files:
        data = []
        for file in files:
            if file.name.endswith('.csv'):
                data.append(parseFile(str(file.path)))
        cleanedData = cleanData(data)
        return cleanedData

def cleanData(data: list) -> list:
    # unique = set()
    dataCopy = []
    for sublist in data:
        for element in sublist:
            dataCopy.append(element)
            # if element['vorname'] not in unique:
            #     dataCopy.append(element)
            #     unique = element['vorname']
    return dataCopy

    # firstnames = {}
    # for sublist in data:
    #     for element in sublist:
    #         metadata = {
    #             1: 0, 
    #             2: 0, 
    #             3: 0, 
    #             4: 0, 
    #             5: 0, 
    #             6: 0, 
    #             7: 0, 
    #             8: 0, 
    #             9: 0,
    #             'geschlecht': 'd',
    #             }

    #         if element['vorname'] not in firstnames:
    #             metadata['geschlecht'] = element['geschlecht']
    #             metadata[element['position']] = element['anzahl']
    #             firstnames[element['vorname']] = metadata
    #         else:
    #             firstnames[element['vorname']][element['position']] += element['anzahl']

    # return firstnames

if __name__ == "__main__":
    startTime = time.time()

    data = parseFilesInDirectory(r".\data\Berlin\2020")
    with Session(engine) as session:
        for element in data:
            # if entry['anzahl'] >= 3:

            resultFirstname = session.execute(
                select(Firstname.uuid, Firstname.firstname).
                where(Firstname.firstname == element['vorname'])
                ).one_or_none()
            
            if resultFirstname is None:
                randomUUID = str(uuid.uuid4())
                firstname = Firstname(
                    uuid = randomUUID,
                    firstname = element['vorname'],
                    gender = element['geschlecht'],
                )
                session.add(firstname)
                
                position = Position(
                    firstname_uuid = randomUUID,
                    amount = element['anzahl'],
                    position = element['position'],
                )
                session.add(position)

            else:
                firstnameUUID = resultFirstname[0]

                resultPosition = session.execute(
                    select(Position.amount)
                    .where(Position.firstname_uuid == firstnameUUID)
                    .where(Position.position == element['position'])
                    ).one_or_none()

                if resultPosition is not None:
                    stmt = session.execute(
                        update(Position.__table__)
                        .where(Position.firstname_uuid == firstnameUUID)
                        .where(Position.position == element['position'])
                        .values(firstname_uuid = firstnameUUID,
                                amount = element['anzahl'] + resultPosition[0])
                    )
                else:
                    stmt = session.execute(
                        insert(Position.__table__).
                        values(firstname_uuid = firstnameUUID,
                        amount = element['anzahl'], position = element['position'])
                    )

        session.commit()
    
    endTime = time.time()
    print(f'Hat {endTime - startTime} Sekunden gedauert')

