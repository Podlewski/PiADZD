import mysql.connector
from os.path import abspath
import sys
from timeit import default_timer as timer

filePath = str(abspath(sys.argv[1])).replace('\\', '/')

setupQuery = f"DROP DATABASE IF EXISTS ServiceRequests; \
                CREATE DATABASE ServiceRequests; \
                USE ServiceRequests; \
                CREATE TABLE sr(\
	                id BIGINT AUTO_INCREMENT PRIMARY KEY, \
                    agencyName NVARCHAR(100), \
                    complaintType NVARCHAR(50), \
                    borough NVARCHAR(50) \
                ); \
                SET GLOBAL local_infile=1; \
                LOAD DATA LOCAL INFILE '{filePath}' \
                INTO TABLE ServiceRequests.sr \
                FIELDS TERMINATED BY ',' \
                OPTIONALLY ENCLOSED BY '\"' \
                LINES TERMINATED BY '\\n' \
                IGNORE 1 LINES \
                (@id, @col, @col, @col, @agencyName, @complaintType, @col, @col, @col, @col, \
                @col, @col, @col, @col, @col, @col, @col, @col, @col, @col, @col, @col, @col, \
                @col, @col, @borough) \
                SET id=@id, agencyName=@agencyName, complaintType=@complaintType, borough=@borough;"

complaintTypeQuery = "SELECT complaintType \
                FROM ServiceRequests.sr \
                GROUP BY complaintType \
                ORDER BY COUNT(*) DESC \
                LIMIT 10;"

agencyNameQuery = "SELECT agencyName \
                FROM ServiceRequests.sr \
                GROUP BY agencyName \
                ORDER BY COUNT(*) DESC \
                LIMIT 10;"

boroughQuery = "SELECT b.borough, b.complaintType \
                FROM (SELECT c.borough, c.complaintType, \
                ROW_NUMBER() OVER(PARTITION BY c.borough ORDER BY c.complaintCount DESC) AS row_num \
                FROM (SELECT complaintType, borough, COUNT(*) AS complaintCount \
                FROM ServiceRequests.sr \
                GROUP BY borough, complaintType) AS c) AS b \
                WHERE row_num <= 10;"

complaintTypeIndex = "CREATE INDEX complaintTypeIndex ON ServiceRequests.sr (complaintType);"
agencyNameIndex = "CREATE INDEX agencyNameIndex ON ServiceRequests.sr (agencyName);"
boroughIndex = "CREATE INDEX complaintTypeBoroughIndex ON ServiceRequests.sr (complaintType, borough);"

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="test",
    allow_local_infile=True
)

cursor = connection.cursor()
sTimer = timer()

for data in cursor.execute(setupQuery, multi=True):
    if data.with_rows:
        data = cursor.fetchall()
lTimer = timer()

cursor.execute(complaintTypeQuery)
cTimer = timer()
cRows = cursor.fetchall()

cursor.execute(agencyNameQuery)
aTimer = timer()
aRows = cursor.fetchall()

cursor.execute(boroughQuery)
bTimer = timer()
bRows = cursor.fetchall()

cursor.execute(complaintTypeIndex)
cursor.execute(agencyNameIndex)
cursor.execute(boroughIndex)
iTimer = timer()

cursor.execute(complaintTypeQuery)
cIndTimer = timer()
cIndRows = cursor.fetchall()

cursor.execute(agencyNameQuery)
aIndTimer = timer()
aIndRows = cursor.fetchall()

cursor.execute(boroughQuery)
bIndTimer = timer()
bIndRows = cursor.fetchall()

cursor.close()
connection.close()

print(f'Loading time:     {lTimer - sTimer:7.2f} s')

print('\n---------- ANALYSIS TIMES -----------------')
print(f'Complaint Type:   {cTimer - lTimer:7.2f} s')
print(f'Boroughs:         {bTimer - aTimer:7.2f} s')
print(f'Agency:           {aTimer - cTimer:7.2f} s')

print()
print(f'Indexing time:    {iTimer - bTimer:7.2f} s')

print('\n---------- ANALYSIS TIMES AFTER INDEXING --')
print(f'Complaint Type:   {cIndTimer - iTimer:7.2f} s')
print(f'Boroughs:         {bIndTimer - aIndTimer:7.2f} s')
print(f'Agency:           {aIndTimer - cIndTimer:7.2f} s')

print('\n---------- ANALYSIS RESULTS ---------------')
print('Complaint Type:')
for row in cRows:
    print(f'\t{row[0]}')

print('\nComplaints Type by Borough:') 
for row in bRows:
    if row[0]:
        boro = row[0] + ':'
        print(f'    {boro:<16}{row[1]}')

print('\nAgency:')
for row in aRows:
    print(f'\t{row[0]}')
