import sys
from datetime import datetime
from timeit import default_timer as timer
import pyodbc
import pandas as pd

print(f'Current Time:\t{datetime.now().strftime("%H:%M:%S")}')
sTimer = timer()
df = pd.read_csv('V:/311_Service_Requests_from_2010_to_Present.csv', usecols=['Agency Name', 'Complaint Type', 'Borough'])
df.to_csv('V:/311_Service_Requests_from_2010_to_Present-cut2.csv', sep='~')

instance_name = sys.argv[1]
connection = pyodbc.connect('Driver={SQL Server};'
                            f'Server={instance_name};'
                            'Database=master;'
                            'Trusted_Connection=yes;',
                            autocommit=True)
cursor = connection.cursor()
cursor.execute("""if exists (select 1 from master..sysdatabases where name='ServiceRequests')
                  begin
                      use master
                      alter database ServiceRequests set single_user with rollback immediate
                      drop database ServiceRequests
                  end""")
cursor.execute("""create database ServiceRequests
                  on (name = ServiceRequests_dat,
                      filename = 'V:\mssql\ServiceRequests.mdf')
                  log
                  on (name = ServiceRequests_log,
                      filename = 'V:\mssql\ServiceRequests.ldf')""")
cursor.execute("""use ServiceRequests
                  create table sr (
                      id              bigint identity (1, 1) primary key,
                      agency          nvarchar(100),
                      complaintType   nvarchar(50),
                      borough         nvarchar(50)
                  )""")
cursor.execute("""bulk insert ServiceRequests..sr
                  from 'V:\\311_Service_Requests_from_2010_to_Present-cut2.csv'
                  with (fieldterminator = '~',
                        rowterminator = '\\n',
                        firstrow = 2)""")
lTimer = timer()

cursor.execute("""select top 10 complaintType
                  from ServiceRequests..sr
                  group by complaintType
                  order by count(*) desc""")
cTimer = timer()
cRows = cursor.fetchall()

cursor.execute("""select top 10 agency
                  from ServiceRequests..sr
                  group by agency
                  order by count(*) desc""")
aTimer = timer()
aRows = cursor.fetchall()

cursor.execute("""select b.borough, b.complaintType
                  from (select c.borough, c.complaintType, row_number() over(partition by c.borough
                                                                             order by c.complaintCount desc) as row_num
                        from (select complaintType, borough, count(*) as complaintCount
                              from ServiceRequests..sr
                              group by borough, complaintType) as c) as b
                  where row_num <= 10""")
bTimer = timer()
bRows = cursor.fetchall()

print('\nTIMES ----------------------------------')
print(f'Loading time:   {lTimer - sTimer:10.3f} s')
print(f'Complaint Type: {cTimer - lTimer:10.3f} s')
print(f'Boroughs:       {bTimer - aTimer:10.3f} s')
print(f'Agency:         {aTimer - cTimer:10.3f} s')
###############################################################################
sTimer = timer()
cursor.execute("""create nonclustered index ix_sra on sr(agency)
                  create nonclustered index ix_src on sr(complaintType)
                  create nonclustered index ic_srb on sr(complaintType, borough)""")
lTimer = timer()

cursor.execute("""select top 10 complaintType
                  from ServiceRequests..sr
                  group by complaintType
                  order by count(*) desc""")
cTimer = timer()
cRows = cursor.fetchall()

cursor.execute("""select top 10 agency
                  from ServiceRequests..sr
                  group by agency
                  order by count(*) desc""")
aTimer = timer()
aRows = cursor.fetchall()

cursor.execute("""select b.borough, b.complaintType
                  from (select c.borough, c.complaintType, row_number() over(partition by c.borough
                                                                             order by c.complaintCount desc) as row_num
                        from (select complaintType, borough, count(*) as complaintCount
                              from ServiceRequests..sr
                              group by borough, complaintType) as c) as b
                  where row_num <= 10""")
bTimer = timer()
bRows = cursor.fetchall()
cursor.close()
connection.close()

print('\nTIMES part 2 ---------------------------')
print(f'Loading time:   {lTimer - sTimer:10.3f} s')
print(f'Complaint Type: {cTimer - lTimer:10.3f} s')
print(f'Boroughs:       {bTimer - aTimer:10.3f} s')
print(f'Agency:         {aTimer - cTimer:10.3f} s')

print('\nANALYSIS RESULTS -----------------------')
print('Complaint type:')
for row in cRows:
    print(f'\t{row[0]}')
print('Complaints type by Borough:')
for row in bRows:
    print(f'\t{row[0]:<15}: {row[1]}')
print('Agency:')
for row in aRows:
    print(f'\t{row[0]}')
