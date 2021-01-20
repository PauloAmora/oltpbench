
create or replace function retrieve_records(retrieve_id integer) returns table(id integer, field0 varchar(100), field1 varchar(100)) as 
$BODY$
declare 
	c1 refcursor;
	rowvar_master master_record%rowtype;
begin
	open c1 for execute 'select * from master_record where id = $1' using retrieve_id;
	loop
		fetch c1 into rowvar_master;
		exit when not found;
		return query execute 'select * from '|| rowvar_master.system || ' where id = $1' using rowvar_master.id_system;
	end loop;
end;
$BODY$
language plpgsql;

select retrieve_records (2)