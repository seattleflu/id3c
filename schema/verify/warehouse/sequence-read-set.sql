-- Verify seattleflu/schema:warehouse/sequence-read-set on pg

begin;

select pg_catalog.has_table_privilege('warehouse.sequence_read_set', 'select');

do $$ begin
    -- Empty urls array
    declare
        _constraint text;
    begin
        with
        sample as (
            insert into warehouse.sample (identifier) values ('__SAMPLE__')
            returning sample_id as id
        )
        insert into warehouse.sequence_read_set (sample_id, urls)
            values ((select id from sample), '{}');

        assert false, 'insert succeeded';
    exception
        when check_violation then
            get stacked diagnostics _constraint = CONSTRAINT_NAME;
            assert _constraint = 'sequence_read_set_urls_is_not_empty', 'wrong constraint';
    end;

end $$;

rollback;
