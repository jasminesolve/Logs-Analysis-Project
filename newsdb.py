import psycopg2

# using the database named "news"
DB = "news"

# connect to the database
conn = psycopg2.connect(database=DB)
cursor = conn.cursor()

# three most popular articles of all time
sql_command_0 = """
select title, count(*) as view_num
from articles, log
where log.path like concat ('%', articles.slug, '%')
group by title
order by view_num desc
limit 3;
"""
cursor.execute(sql_command_0)
r = cursor.fetchall()
print("What's the most popular three articles of all time?")
print(r)
print("\n")

# the most popular article authors of all time
# get authors' ids from articles&log and put them into view PA
sql_command_1 = """
create view PA as
select author, count(*) as author_view
from articles, log
where log.path like concat ('%', articles.slug, '%')
group by author
order by author_view desc;
"""
# get authors' names from authors and list the top 3
sql_command_2 = """
select name, author_view
from authors, PA
where authors.id = PA.author
order by author_view desc
limit 4;
"""

cursor.execute(sql_command_1)
cursor.execute(sql_command_2)
r = cursor.fetchall()
print("Who are the most popular article authors of all time?")
print(r)
print("\n")

# the highest error rate day of all time
# sum up requests for everyday and put them into view DR
sql_command_3 = """
create view DR as
select time::timestamp::date as day, count(*) as daily_requests
from log
group by day
order by day;
"""
# sum up errors for everyday and put them into view ER
sql_command_4 = """
create view ER as
select time::timestamp::date as day, count(*) as error_requests
from log
where status != '200 OK'
group by day
order by day;
"""
# combine view DR & ER
sql_command_5 = """
create view RT as
select ER.day as day, error_requests,daily_requests
from DR, ER
where ER.day = DR.day;
"""
# calculate error rate for everyday and got the top 1
sql_command_6 = """
select day, error_requests/daily_requests::float as er
from RT
order by er desc
limit 1;
"""

cursor.execute(sql_command_3)
cursor.execute(sql_command_4)
cursor.execute(sql_command_5)
cursor.execute(sql_command_6)

r = cursor.fetchall()
conn.close()
print("On which days did more than '1%' of requests lead to errors?")
print(r)
print("\n")
