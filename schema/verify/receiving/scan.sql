-- Verify seattleflu/schema:receiving/scan on pg

begin;

do $$
    declare
        existing_tables int;
    begin
        select into existing_tables count(*)
          from information_schema.tables
         where table_schema = 'receiving'
           and table_name in ('scan_set', 'collection', 'sample', 'aliquot');

        assert existing_tables = 0;
    end
$$;

rollback;
