import psycopg2, datetime
from pathlib import Path
import os
os.system('./download_file.sh')
os.system('gcc -o create_tables create_tables.c -I/usr/include/postgresql -lpq -std=c99')
os.system('./create_tables')

def localtime():
    current_time = datetime.datetime.now()
    date, time = str(current_time).split()
    time, remain = time.split(".")
    return date, time


date, time = localtime()
path_folder = 'results/'+date+'/'+time
Path(path_folder).mkdir(parents = True, exist_ok = True)
conn = psycopg2.connect(database = "fpdb", user = "postgres", password = "@Mahdi773155", host = "127.0.0.1", port = "5432")
cur = conn.cursor()



cur.execute('''	SELECT Distinct on (province) province,product_id,sum(has_sold) AS number_of_sales,(SUM(quantity*price))/SUM(quantity) AS average_price
	FROM fp_stores_data
	GROUP BY province,product_id
	ORDER BY province,number_of_sales DESC;''')

path = path_folder+"/most_number_of_product_sales_in_every_province.txt"
f = open(path, "a")
f.write("province,product_id, number_of_sales, price\n")
rows = cur.fetchall()
for row in rows:
    f.write(str(row[0])+","+str(row[1])+","+str(row[2])+","+str(row[3])+"\n")
f.close()



cur.execute('''	SELECT market_id,sum(has_sold*price) AS sales_price
    FROM fp_stores_data
    GROUP BY market_id
    ORDER BY sales_price DESC;''')
path = path_folder+"/most_sales_of_market.txt"
f = open(path, "a")
f.write("market_id,sales_price\n")
rows = cur.fetchall()
for row in rows:
    f.write(str(row[0])+","+str(row[1])+"\n")
f.close()




cur.execute('''	SELECT Distinct on (market_id) market_id,product_id,sum(has_sold) AS number_of_sales,(SUM(quantity*price))/SUM(quantity) AS average_price
	FROM fp_stores_data
	GROUP BY market_id,product_id
	ORDER BY market_id,number_of_sales DESC;''')
path = path_folder+"/most_number_of_product_sales_in_every_market.txt"
f = open(path, "a")
f.write("market_id,product_id, number_of_sales, price\n")
rows = cur.fetchall()
for row in rows:
    f.write(str(row[0])+","+str(row[1])+","+str(row[2])+","+str(row[3])+"\n")
f.close()




cur.execute('''SELECT city,SUM(has_sold*price) AS sales_price
FROM fp_stores_data
GROUP BY city
ORDER BY sales_price DESC;''')
path = path_folder+"/most_sales_of_city.txt"
f = open(path, "a")
f.write("city, sales_price\n")
rows = cur.fetchall()
for row in rows:
    f.write(str(row[0])+","+str(row[1])+"\n")
f.close()





cur.execute('''SELECT province,sum(has_sold*price) AS sales_price
FROM fp_stores_data
GROUP BY province
ORDER BY sales_price DESC;''')
path = path_folder+"/most_sales_of_province.txt"
f = open(path, "a")
f.write("province, sales_price\n")
rows = cur.fetchall()
for row in rows:
    f.write(str(row[0])+","+str(row[1])+"\n")
f.close()





cur.execute('''SELECT market_id,total_sales_price
FROM fp_store_aggregation
ORDER BY total_sales_price DESC;''')
path = path_folder+"/most_sales_of_market_in_last_30_minutes.txt"
f = open(path, "a")
f.write("market_id, sales_price\n")
rows = cur.fetchall()
for row in rows:
    f.write(str(row[0])+","+str(row[1])+"\n")
f.close()
cur.close()
conn.commit()
conn.close()
