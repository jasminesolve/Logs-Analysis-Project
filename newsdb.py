#!/usr/bin/env python3.
import psycopg2

# using the database named "news"
DB = "news"

# connect to the database
conn = psycopg2.connect(database=DB)
cursor = conn.cursor()

# three most popular articles of all time
sql_command_0 = """
SELECT title, count(*) AS view_num
FROM articles, log
WHERE log.path LIKE concat ('%', articles.slug, '%')
GROUP BY title
ORDER BY view_num DESC
LIMIT 3;
"""
cursor.execute(sql_command_0)
r = cursor.fetchall()
print("What's the most popular three articles of all time?")
print(r[0][0]+" "+"-"+str(r[0][1])+"views"+"\n")
print(r[1][0]+" "+"-"+str(r[1][1])+"views"+"\n")
print(r[2][0]+" "+"-"+str(r[2][1])+"views"+"\n")

# the most popular article authors of all time
# get authors' ids from articles&log and put them into view PA
sql_command_1 = """
CREATE VIEW PA AS
SELECT author, count(*) AS author_view
from articles, log
WHERE log.path LIKE concat ('%', articles.slug, '%')
GROUP BY author
ORDER BY author_view DESC;
"""
# get authors' names from authors and list the top 3
sql_command_2 = """
SELECT name, author_view
FROM authors, PA
WHERE authors.id = PA.author
ORDER BY author_view DESC
LIMIT 4;
"""

cursor.execute(sql_command_1)
cursor.execute(sql_command_2)
r = cursor.fetchall()
print("Who are the most popular article authors of all time?")
print(r[0][0]+" "+"-"+str(r[0][1])+"views"+"\n")
print(r[1][0]+" "+"-"+str(r[1][1])+"views"+"\n")
print(r[2][0]+" "+"-"+str(r[2][1])+"views"+"\n")
print(r[3][0]+" "+"-"+str(r[3][1])+"views"+"\n")

# the highest error rate day of all time
# sum up requests for everyday and put them into view DR
sql_command_3 = """
CREATE VIEW DR AS
SELECT time::timestamp::date AS day, count(*) AS daily_requests
FROM log
GROUP BY day
ORDER BY day;
"""
# sum up errors for everyday and put them into view ER
sql_command_4 = """
CREATE VIEW ER AS
SELECT time::timestamp::date AS day, count(*) AS error_requests
FROM log
WHERE status != '200 OK'
GROUP BY day
ORDER BY day;
"""
# combine view DR & ER
sql_command_5 = """
CREATE VIEW RT AS
SELECT ER.day AS day, error_requests,daily_requests
FROM DR, ER
WHERE ER.day = DR.day;
"""
# calculate error rate for everyday and got the top 1
sql_command_6 = """
SELECT day, error_requests/daily_requests::float AS er
FROM RT
ORDER BY er desc
LIMIT 1;
"""

cursor.execute(sql_command_3)
cursor.execute(sql_command_4)
cursor.execute(sql_command_5)
cursor.execute(sql_command_6)

r = cursor.fetchall()
conn.close()
print("On which days did more than '1%' of requests lead to errors?")
print(str(r[0][0])+" "+str(int(r[0][1]*100))+"%"+"\n")
