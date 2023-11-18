"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import os

import psycopg2

# подключение к бд
conn = psycopg2.connect(
    host="localhost",
    database="north",
    user="postgres",
    password="721719"
)

# создание объекта курсора
cur = conn.cursor()

# считывание с папки north_data файлов с расширением .csv ( execute - запрос построчно, поэлементно)
file_path =os.path.join("north_data","employees_data.csv")
with open(file_path, 'r', encoding='windows-1251') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        cur.execute("INSERT INTO employees_data VALUES(%s, %s, %s, %s, %s, %s)",(row["employee_id"], row["first_name"], row["last_name"], row["title"], row["birth_date"], row["notes"]))

file_path =os.path.join("north_data","customers_data.csv")
with open(file_path, 'r', encoding='windows-1251') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        cur.execute("INSERT INTO customers_data VALUES(%s, %s, %s)",(row["customer_id"], row["company_name"], row["contact_name"]))

file_path =os.path.join("north_data","orders_data.csv")
with open(file_path, 'r', encoding='windows-1251') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        cur.execute("INSERT INTO orders_data VALUES(%s, %s, %s, %s, %s)",(row["order_id"], row["customer_id"], row["employee_id"], row["order_date"], row["ship_city"]))

# закоммитить на бд
conn.commit()
# закрытие курсора и соединения с бд
cur.close()
conn.close()



