import os
import pandas as pd
import pyodbc
import sys
from timeit import default_timer as timer

filePath = sys.argv[1]
newFilePath = str(os.path.abspath(filePath[:-4] + '-cut.csv')).replace('\\', '/') 
instanceName = sys.argv[2]
serverDir = str(os.path.abspath(sys.argv[3]))

if not os.path.exists(serverDir):
    os.makedirs(serverDir)

sTimer = timer()
df = pd.read_csv(filePath, usecols=['Agency Name', 'Complaint Type', 'Borough'])
df.to_csv(newFilePath, sep='~')

connection = pyodbc.connect('Driver={SQL Server};'
                            f'Server={instanceName};'
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
cursor.execute(f"""create database ServiceRequests
                   on (name = ServiceRequests_dat,
                       filename = '{serverDir}\ServiceRequests.mdf')
                   log
                   on (name = ServiceRequests_log,
                       filename = '{serverDir}\ServiceRequests.ldf')""")
cursor.execute("""use ServiceRequests
                  create table sr (
                      id              bigint identity (1, 1) primary key,
                      agency          nvarchar(100),
                      complaintType   nvarchar(50),
                      borough         nvarchar(50)
                  )""")
cursor.execute(f"""bulk insert ServiceRequests..sr
                   from '{newFilePath}'
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
print(f'Loading time:     {lTimer - sTimer:7.2f} s')

print('\n---------- ANALYSIS TIMES -----------------')
print(f'Complaint Type:   {cTimer - lTimer:7.2f} s')
print(f'Boroughs:         {bTimer - aTimer:7.2f} s')
print(f'Agency:           {aTimer - cTimer:7.2f} s')
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

print()
print(f'Indexing time:    {lTimer - sTimer:7.2f} s')

print('\n---------- ANALYSIS TIMES AFTER INDEXING --')
print(f'Complaint Type:   {cTimer - lTimer:7.2f} s')
print(f'Boroughs:         {bTimer - aTimer:7.2f} s')
print(f'Agency:           {aTimer - cTimer:7.2f} s')

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
