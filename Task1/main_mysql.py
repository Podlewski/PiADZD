import pyodbc
from datetime import datetime
from timeit import default_timer as timer

connection = pyodbc.connect('Driver=MySQL ODBC 8.0 ANSI Driver;'
                            'Server=localhost;'
                            'Database=ServiceRequests;'
                            'UID=root;'
                            'PWD=test;')

cursor = connection.cursor()
print(f'Current Time:\t{datetime.now().strftime("%H:%M:%S")}')
sTimer = timer()

cursor.execute("SELECT complaintType \
                FROM ServiceRequests.sr \
                GROUP BY complaintType \
                ORDER BY COUNT(*) DESC \
                LIMIT 1;")
cTimer = timer()
cRows = cursor.fetchall()

cursor.execute("SELECT agencyName \
                FROM ServiceRequests.sr \
                GROUP BY agencyName \
                ORDER BY COUNT(*) DESC \
                LIMIT 1;")
aTimer = timer()
aRows = cursor.fetchall()

cursor.execute("SELECT b.borough, b.complaintType \
                FROM (SELECT c.borough, c.complaintType, \
                ROW_NUMBER() OVER(PARTITION BY c.borough ORDER BY c.complaintCount DESC) AS row_num \
                FROM (SELECT complaintType, borough, COUNT(*) AS complaintCount \
                FROM ServiceRequests.sr \
                GROUP BY borough, complaintType) AS c) AS b \
                WHERE row_num = 1;")
bTimer = timer()
bRows = cursor.fetchall()

print('\nANALYSIS TIMES -------------------------')
print(f'Complaint Type:{cTimer - sTimer:10.3f} s')
print(f'Boroughs:      {bTimer - aTimer:10.3f} s')
print(f'Agency:        {aTimer - cTimer:10.3f} s')

print('\nANALYSIS RESULTS -----------------------')
print('Complaint type:')
for row in cRows:
    print(f'\t{row[0]}')
print('Complaints type by Borough:')
for row in bRows[1:]:
    print(f'\t{row[0]:<15}:{row[1]:>20}')
print('Agency:')
for row in aRows:
    print(f'\t{row[0]}')
