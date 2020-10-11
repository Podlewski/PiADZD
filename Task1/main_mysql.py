import mysql.connector
from datetime import datetime
from timeit import default_timer as timer

db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="test",
    database="ServiceRequests"
)

cursor = db.cursor()
print(f'Current Time:\t{datetime.now().strftime("%H:%M:%S")}')
sTimer = timer()

cursor.execute("SELECT complaintType \
                FROM ServiceRequests.sr \
                GROUP BY complaintType \
                ORDER BY COUNT(*) DESC \
                LIMIT 1;")
cTimer = timer()
cRows = cursor.fetchall()

cursor.execute("SELECT Agency \
                FROM ServiceRequests.sr \
                GROUP BY Agency \
                ORDER BY COUNT(*) DESC \
                LIMIT 1;")
aTimer = timer()
aRows = cursor.fetchall()

print('\nANALYSIS TIMES -------------------------')
print(f'Complaint Type:{cTimer - sTimer:10.3f} s')
# print(f'Boroughs:      {bTimer - aTimer:10.3f} s')
print(f'Agency:        {aTimer - cTimer:10.3f} s')

print('\nANALYSIS RESULTS -----------------------')
print('Complaint type:')
for row in cRows:
    print(f'\t{row[0]}')
# print('Complaints type by Borough:')
# for row in bRows:
#     print(f'\t{row[0]:<15}:{row[1]:>20}')
print('Agency:')
for row in aRows:
    print(f'\t{row[0]}')