import mysql.connector
from timeit import default_timer as timer

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="test",
    allow_local_infile=True
)

cursor = connection.cursor()
sTimer = timer()

query = "DROP DATABASE IF EXISTS ServiceRequests; \
                CREATE DATABASE ServiceRequests; \
                USE ServiceRequests; \
                CREATE TABLE sr(\
	                id BIGINT AUTO_INCREMENT PRIMARY KEY, \
                    agencyName NVARCHAR(100), \
                    complaintType NVARCHAR(50), \
                    borough NVARCHAR(50) \
                ); \
                SET GLOBAL local_infile=1; \
                LOAD DATA LOCAL INFILE 'D:/311_Service_Requests_from_2010_to_Present.csv' \
                INTO TABLE ServiceRequests.sr \
                FIELDS TERMINATED BY ',' \
                OPTIONALLY ENCLOSED BY '\"' \
                LINES TERMINATED BY '\\n' \
                IGNORE 1 LINES \
                (@id, @col, @col, @col, @agencyName, @complaintType, @col, @col, @col, @col, \
                @col, @col, @col, @col, @col, @col, @col, @col, @col, @col, @col, @col, @col, \
                @col, @col, @borough) \
                SET id=@id, agencyName=@agencyName, complaintType=@complaintType, borough=@borough;"

for data in cursor.execute(query, multi=True):
    if data.with_rows:
        data = cursor.fetchall()

lTimer = timer()

cursor.execute("SELECT complaintType \
                FROM ServiceRequests.sr \
                GROUP BY complaintType \
                ORDER BY COUNT(*) DESC \
                LIMIT 10;")
cTimer = timer()
cRows = cursor.fetchall()

cursor.execute("SELECT agencyName \
                FROM ServiceRequests.sr \
                GROUP BY agencyName \
                ORDER BY COUNT(*) DESC \
                LIMIT 10;")
aTimer = timer()
aRows = cursor.fetchall()

cursor.execute("SELECT b.borough, b.complaintType \
                FROM (SELECT c.borough, c.complaintType, \
                ROW_NUMBER() OVER(PARTITION BY c.borough ORDER BY c.complaintCount DESC) AS row_num \
                FROM (SELECT complaintType, borough, COUNT(*) AS complaintCount \
                FROM ServiceRequests.sr \
                GROUP BY borough, complaintType) AS c) AS b \
                WHERE row_num <= 10;")
bTimer = timer()
bRows = cursor.fetchall()

cursor.close()
connection.close()

print(f'Loading time:     {lTimer - sTimer:7.2f} s')

print('\nANALYSIS TIMES -------------------------')
print(f'Complaint Type:{cTimer - lTimer:10.3f} s')
print(f'Boroughs:      {bTimer - aTimer:10.3f} s')
print(f'Agency:        {aTimer - cTimer:10.3f} s')

print('\nANALYSIS RESULTS -----------------------')
print('Complaint type:')
for row in cRows:
    print(f'\t{row[0]}')
print('Complaints type by Borough:')
for row in bRows:
    if row[0]:
        print(f'\t{row[0]:<15}:{row[1]:>20}')
print('Agency:')
for row in aRows:
    print(f'\t{row[0]}')
