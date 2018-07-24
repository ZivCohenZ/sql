use database_name
select distinct name,definition  from sys.procedures proc1
inner join sys.sql_modules sqlm on proc1.object_id=sqlm.object_id
where 
definition like '%vasr1%'  
order by  name




SELECT t.name AS table_name,
c.name AS column_name
FROM sys.tables AS t
INNER JOIN sys.columns c ON t.OBJECT_ID = c.OBJECT_ID
where c.name like '%var%' 
