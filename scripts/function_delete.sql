create or replace function delete_records(delete_id integer) returns void as 
$BODY$
declare 
	c1 refcursor;
	rowvar_master master_record%rowtype;
begin
	open c1 for execute 'select * from master_record where id = $1' using delete_id;
	loop
		fetch c1 into rowvar_master;
		exit when not found;
		execute 'delete from ' || rowvar_master.system || ' where id = $1' using rowvar_master.id_system;
		DELETE FROM master_record WHERE CURRENT OF c1;
	end loop;
end;
$BODY$
language plpgsql;