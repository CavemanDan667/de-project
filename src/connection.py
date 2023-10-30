from dotenv import dotenv_values
import psycopg2
import csv

config = dotenv_values(".env")

con = psycopg2.connect(
    database=config["DATABASE"],
    user=config["USER"],
    password=config["PASSWORD"],
    host=config["HOST"],
    port=config["PORT"],
)


def testing():
    cursor_obj = con.cursor()
    cursor_obj.execute("SELECT * FROM currency")
    result = cursor_obj.fetchall()
    filepath = 'data.csv'
    csvfile = open(filepath, 'w', newline="")
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(col[0] for col in cursor_obj.description)
    csv_writer.writerows(result)

    cursor_obj.close()
    con.close()


testing()
