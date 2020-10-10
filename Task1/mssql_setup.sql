-- SELECT SUBSTRING(physical_name, 1, CHARINDEX(N'master.mdf', LOWER(physical_name)) - 1)
-- FROM master.sys.master_files
-- WHERE database_id = 1 AND file_id = 1
-- GO

if exists (select 1 from master..sysdatabases where name='ServiceRequests')
begin
    use master
    alter database ServiceRequests set single_user with rollback immediate
    drop database ServiceRequests
end
go

create database ServiceRequests
on (name = ServiceRequests_dat,
    filename = 'V:\mssql\ServiceRequests.mdf')
log
on (name = ServiceRequests_log,
    filename = 'V:\mssql\ServiceRequests.ldf')
go

use ServiceRequests
go
create table sr (
    id              bigint identity (1, 1) primary key,
    agency          nvarchar(50),
    complaintType   nvarchar(50),
    borough         nvarchar(50)
)
go

bulk insert ServiceRequests..sr
from 'V:\311_Service_Requests_from_2010_to_Present-cut.csv'
with (fieldterminator = ',',
      rowterminator = '\n',
      firstrow = 2)
go



select count(*) from ServiceRequests..sr
select top 10 * from ServiceRequests..sr

-- 1
select top 1 complaintType
from ServiceRequests..sr
group by complaintType
order by count(*) desc

-- 3
select top 1 agency
from ServiceRequests..sr
group by agency
order by count(*) desc

-- 2
select srout.borough, (select top 1 srin.complaintType
                       from ServiceRequests..sr as srin
                       where srin.borough = srout.borough
                       group by srin.complaintType
                       order by count(*) desc) as 'complaintType'
from ServiceRequests..sr as srout
group by srout.borough


