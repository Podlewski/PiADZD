import mysql.connector

db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="test",
    database="task1"
)

cursor = db.cursor()

cursor.execute("SELECT COUNT(*), `Complaint Type` \
FROM task1.requests \
GROUP BY `Complaint Type` \
ORDER BY COUNT(*) desc \
LIMIT 1;")

result = cursor.fetchall()

for x in result:
    print(x)