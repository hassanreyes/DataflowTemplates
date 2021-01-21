#!/usr/bin/python
import psycopg2
import random
import time

connection = psycopg2.connect(host="127.0.0.1",
                        user="",
                        password="",
                        database="postgres",
                        port=5432)
MAX_RECORDS = 100

with connection:
    for i in range(0, MAX_RECORDS):
        wait = random.uniform(0, 4)
        print('Generating random order {} of {} in {:.2f} sec'.format(i, MAX_RECORDS, wait))
        time.sleep(wait)
        with connection.cursor() as cursor:
            # Create a new record
            sql = """
            INSERT INTO 
                inventory.orders (order_date, purchaser, quantity, product_id )
            VALUES (
                CAST (CONCAT('2021-01-', FLOOR(RANDOM()*(31)+1)) AS DATE),
                ( SELECT customers.id FROM inventory.customers ORDER BY RANDOM() LIMIT 1),
                FLOOR(RANDOM()*(15)+10),
                ( SELECT products.id FROM inventory.products ORDER BY RANDOM() LIMIT 1)
            )
            """
            cursor.execute(sql)
        connection.commit()