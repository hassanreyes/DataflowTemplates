#!/usr/bin/python
import pymysql.cursors
import random
import time

connection = pymysql.connect(host="34.66.161.43",
                        user="",
                        password="",
                        database="inventory",
                        port=3306)
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
                orders (order_date, purchaser, quantity, product_id )
            VALUES (
                CONCAT('2021-01-', FLOOR(RAND()*(31)+1)),
                ( SELECT customers.id FROM customers ORDER BY RAND() LIMIT 1),
                FLOOR(RAND()*(15)+10),
                ( SELECT products.id FROM products ORDER BY RAND() LIMIT 1)
            )
            """
            cursor.execute(sql)
        connection.commit()