create database if not exists ServiceRequests;

use ServiceRequests;
create table sr(
	id bigint auto_increment primary key,
    agency nvarchar(50),
    complaintType nvarchar(50),
    borough nvarchar(50)
);

set global local_infile=1;

load data local infile 'D:/311_Service_Requests_from_2010_to_Present.csv' 
into table ServiceRequests.sr
fields terminated by ',' 
optionally enclosed by '"'
lines terminated by '\n'
ignore 1 lines
(@id, @col, @col, @agency, @col, @complaintType, @col, @col, @col, @col, @col, @col, @col,
@col, @col, @col, @col, @col, @col, @col, @col, @col, @col, @col, @col, @borough)
set id=@id, agency=@agency, complaintType=@complaintType, borough=@borough;