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
    agency          nvarchar(100),
    complaintType   nvarchar(50),
    borough         nvarchar(50)
)
go

create nonclustered index ix_sra on sr(agency asc)
create nonclustered index ix_src on sr(complaintType asc)
create nonclustered index ic_srb on sr(borough asc)

bulk insert ServiceRequests..sr
from 'V:\311_Service_Requests_from_2010_to_Present-cut2.csv'
with (fieldterminator = '~',
      rowterminator = '\n',
      firstrow = 2)
go



select count(*) from ServiceRequests..sr
select top 10 * from ServiceRequests..sr

-- 1
select top 10 complaintType
from ServiceRequests..sr
group by complaintType
order by count(*) desc

-- 3
select top 10 agency
from ServiceRequests..sr
group by agency
order by count(*) desc

-- 2
select b.borough, b.complaintType
from (select c.borough, c.complaintType, row_number() over(partition by c.borough
                                                           order by c.complaintCount desc) as row_num
      from (select complaintType, borough, count(*) as complaintCount
            from ServiceRequests..sr
            group by borough, complaintType) as c) as b
where row_num <= 10;


