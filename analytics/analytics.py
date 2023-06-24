from os import environ
from time import sleep
from sqlalchemy import create_engine ,Table, Column, Integer, String, Float, MetaData, JSON, func, DateTime
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.mysql import insert
from math import sin, cos, acos, radians
from builtins import zip

print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL and MySQL successful.')


print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')



# Define the table structures for PostgreSQL and MySQL
psql_metadata = MetaData(psql_engine)
mysql_metadata = MetaData(mysql_engine)

Base = declarative_base()

class Devices(Base):
    __table__ = Table('devices', psql_metadata,
        Column('device_id', Integer, primary_key=True),
        Column('time', DateTime),
        Column('location', Float)
    )

class AggregatedData(Base):
    __table__ = Table('aggregated_data', mysql_metadata,
        Column('device_id', Integer, primary_key=True),
        Column('hour', DateTime, primary_key=True),
        Column('max_temperature', Float),
        Column('data_points', Integer),
        Column('total_distance', Float)
    )

async def perform_data_etl():
                psql_session = sessionmaker(bind=psql_engine)()
                mysql_session = sessionmaker(bind=mysql_engine)()
    while True:
        try:
            # Calculate maximum temperatures per hour for every device
            max_temperatures = calculate_maximum_temperatures_per_hour(psql_session)
            print('Maximum Temperatures:', max_temperatures)

            # Calculate data points aggregated per hour for every device
            data_points_aggregated = calculate_data_points_aggregated_per_hour(psql_session)
            print('Data Points Aggregated:', data_points_aggregated)

            # Calculate total distance of device movement per hour for every device
            total_distance = calculate_total_distance_per_hour(psql_session)
            print('Total Distance:', total_distance)

            # Store the aggregated data into MySQL
            store_aggregated_data(mysql_session, max_temperatures, data_points_aggregated, total_distance)

        except OperationalError:
            sleep(0.1)

def calculate_maximum_temperatures_per_hour(psql_session):
    devices = Devices.__table__

    # Query to calculate maximum temperatures per hour for every device
    query = select(
        devices.c.device_id,
        func.date_trunc('hour', devices.c.time).label('hour'),
        func.max(devices.c.temperature).label('max_temperature')
    ).group_by(
        devices.c.device_id,
        func.date_trunc('hour', devices.c.time)
    )

    # Execute the query and fetch all rows
    rows = psql_session.execute(query).fetchall()

    result = []
    for row in rows:
        device_id = row.device_id
        hour = row.hour
        max_temperature = row.max_temperature

        result.append({
            'device_id': device_id,
            'hour': hour,
            'max_temperature': max_temperature
        })

    return result

def calculate_data_points_aggregated_per_hour(psql_session):
    devices = Devices.__table__

    # Query to calculate data points aggregated per hour for every device
    query = select(
        devices.c.device_id,
        func.date_trunc('hour', devices.c.time).label('hour'),
        func.count().label('data_points')
    ).group_by(
        devices.c.device_id,
        func.date_trunc('hour', devices.c.time)
    )

    # Execute the query and fetch all rows
    rows = psql_session.execute(query).fetchall()

    result = []
    for row in rows:
        device_id = row.device_id
        hour = row.hour
        data_points = row.data_points

        result.append({
            'device_id': device_id,
            'hour': hour,
            'data_points': data_points
        })

    return result

def calculate_total_distance_per_hour(psql_session):
    devices = Devices.__table__

    # Query to calculate the total distance of device movement per hour for every device
    query = select(
        devices.c.device_id,
        func.date_trunc('hour', devices.c.time).label('hour'),
        func.sum(
            func.acos(
                func.sin(func.radians(devices.c.latitude)) * func.sin(func.radians(devices.c.latitude_next))) +
                func.cos(func.radians(devices.c.latitude)) * func.cos(func.radians(devices.c.latitude_next)) *
                func.cos(func.radians(devices.c.longitude) - func.radians(devices.c.longitude_next))
            ) * 6371
        ).label('total_distance')
    ).select_from(
        devices.join(
            devices.alias('next'),
            devices.c.device_id == devices.alias('next').c.device_id,
            devices.c.time < devices.alias('next').c.time
        )
    ).where(
        func.date_trunc('hour', devices.c.time) == func.date_trunc('hour', devices.alias('next').c.time)
    ).group_by(
        devices.c.device_id,
        func.date_trunc('hour', devices.c.time)
    )

    # Execute the query and fetch all rows
    rows = psql_session.execute(query).fetchall()

    result = []
    for row in rows:
        device_id = row.device_id
        hour = row.hour
        total_distance = row.total_distance

        result.append({
            'device_id': device_id,
            'hour': hour,
            'total_distance': total_distance
        })

    return result

def store_aggregated_data(mysql_session, max_temperatures, data_points_aggregated, total_distance):
    aggregated_data = AggregatedData.__table__

    # Create a list of dictionaries representing the rows to be inserted
    rows = []
    for max_temp, data_points, distance in zip(max_temperatures, data_points_aggregated, total_distance):
        device_id = max_temp[0]
        hour = max_temp[1]
        max_temperature = max_temp[2]
        data_points_value = data_points[2]
        total_distance_value = distance[2]

        row = {
            'device_id': device_id,
            'hour': hour,
            'max_temperature': max_temperature,
            'data_points': data_points_value,
            'total_distance': total_distance_value
        }
        rows.append(row)

    # Insert the rows into the MySQL table
    with mysql_session.begin():
        for row in rows:
            insert_stmt = insert(aggregated_data).values(row)
            on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(
                max_temperature=insert_stmt.inserted.max_temperature,
                data_points=insert_stmt.inserted.data_points,
                total_distance=insert_stmt.inserted.total_distance
            )
            mysql_session.execute(on_duplicate_key_stmt)

if __name__ == "__main__":
    asyncio.run(perform_data_etl())
