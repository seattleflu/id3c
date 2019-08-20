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

    -- Null urls
    declare
        _constraint text;
    begin
        with
        sample as (
            insert into warehouse.sample (identifier) values ('__SAMPLE2__')
            returning sample_id as id
        )
        insert into warehouse.sequence_read_set (sample_id, urls)
            values((select id from sample), '{"__URL__", null}');

        assert false, 'insert succeeded';
    exception
        when check_violation then
            get stacked diagnostics _constraint = CONSTRAINT_NAME;
            assert _constraint = 'sequence_read_set_urls_contains_no_nulls', 'wrong constraint';
    end;

    -- Duplicate urls within the same sequence read set
    declare
        _constraint text;
    begin
        with
        sample as (
            insert into warehouse.sample (identifier) values ('__SAMPLE3__')
            returning sample_id as id
        )
        insert into warehouse.sequence_read_set (sample_id, urls)
            values ((select id from sample), '{"__URL1__", "__URL1__"}');

        assert false, 'insert succeeded';
    exception
        when check_violation then
            get stacked diagnostics _constraint = CONSTRAINT_NAME;
            assert _constraint = 'sequence_read_set_urls_are_unique', 'wrong constraint';
    end;

end $$;

rollback;
