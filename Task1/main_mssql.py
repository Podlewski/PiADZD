import sys
from datetime import datetime
from timeit import default_timer as timer
import pyodbc

instance_name = sys.argv[1]
connection = pyodbc.connect('Driver={SQL Server};'
                            f'Server={instance_name};'
                            'Database=ServiceRequests;'
                            'Trusted_Connection=yes;')
cursor = connection.cursor()
print(f'Current Time:\t{datetime.now().strftime("%H:%M:%S")}')
sTimer = timer()

cursor.execute('select top 1 complaintType '
               'from ServiceRequests..sr '
               'group by complaintType '
               'order by count(*) desc')
cTimer = timer()
cRows = cursor.fetchall()

cursor.execute('select top 1 agency '
               'from ServiceRequests..sr '
               'group by agency '
               'order by count(*) desc')
aTimer = timer()
aRows = cursor.fetchall()

cursor.execute('select srout.borough, (select top 1 srin.complaintType '
               'from ServiceRequests..sr as srin '
               'where srin.borough = srout.borough '
               'group by srin.complaintType '
               'order by count(*) desc) as "complaintType" '
               'from ServiceRequests..sr as srout '
               'group by srout.borough')
bTimer = timer()
bRows = cursor.fetchall()
cursor.close()
connection.close()

print('\nANALYSIS TIMES -------------------------')
print(f'Complaint Type:{cTimer - sTimer:10.3f} s')
print(f'Boroughs:      {bTimer - aTimer:10.3f} s')
print(f'Agency:        {aTimer - cTimer:10.3f} s')

print('\nANALYSIS RESULTS -----------------------')
print('Complaint type:')
for row in cRows:
    print(f'\t{row[0]}')
print('Complaints type by Borough:')
for row in bRows:
    print(f'\t{row[0]:<15}:{row[1]:>20}')
print('Agency:')
for row in aRows:
    print(f'\t{row[0]}')
