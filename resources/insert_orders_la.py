#!/usr/bin/python
import pymysql.cursors
import random
import time
import sys
import argparse

parser = argparse.ArgumentParser(description="Inserts fake orders.")
parser.add_argument("--host", help="the database host", default="127.0.0.1")
parser.add_argument('-p', '--port', type=int, help="the database port", default=3306)
parser.add_argument('-u', '--user', help="the database user", default="root")
parser.add_argument('-w', '--password', help="the database user's password", default="debezium")
parser.add_argument('-y', '--year', type=int, help="year for fake orders", default=2020)
parser.add_argument('-n', '--max_records', type=int, help="max number of fake orders", default=100)

args = parser.parse_args()

connection = pymysql.connect(host=args.host,
                        user=args.user,
                        password=args.password,
                        database="inventory",
                        port=args.port)

with connection:
    for i in range(0, args.max_records):
        wait = random.uniform(0, 4)
        print('Generating random order {} of {} in {:.2f} sec'.format(i, args.max_records, wait))
        time.sleep(wait)
        with connection.cursor() as cursor:
            # Create a new record
            sql = """
            INSERT INTO 
                la_online_orders (order_datetime_pst, purchaser, quantity, product_id )
            VALUES (
                CONCAT('%s-', FLOOR(RAND()*(12)+1), '-', FLOOR(RAND()*(29)+1), ' ', FLOOR(RAND()*(12)+1), ':', FLOOR(RAND()*(59)+1), ':', FLOOR(RAND()*(59)+1)),
                ( SELECT customers.id FROM customers ORDER BY RAND() LIMIT 1),
                FLOOR(RAND()*(15)+10),
                ( SELECT products.id FROM products ORDER BY RAND() LIMIT 1)
            )
            """ % (args.year)
            cursor.execute(sql)
        connection.commit()