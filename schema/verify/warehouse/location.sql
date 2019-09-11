-- Verify seattleflu/schema:warehouse/location on pg

begin;

select pg_catalog.has_table_privilege('warehouse.location', 'select');

do $$ begin
    declare
        _constraint text;
    begin
        -- Ensure uniqueness on (identifier, scale)
        insert into warehouse.location (identifier, scale)
            values ('Null Island', 'nation');

        -- Ok because scale is different
        insert into warehouse.location (identifier, scale)
            values ('Null Island', 'country');

        -- Not Ok because tuple is the same case-insensitively
        insert into warehouse.location (identifier, scale)
            values ('null island', 'nation');

        assert false, 'insert succeeded';
    exception
        when unique_violation then
            get stacked diagnostics _constraint = CONSTRAINT_NAME;
            assert _constraint = 'location_scale_identifier_key', 'wrong constraint';
    end;
end $$;

rollback;
