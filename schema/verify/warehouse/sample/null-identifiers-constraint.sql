-- Verify seattleflu/schema:warehouse/sample/null-identifiers-constraint on pg

begin;

do $$ begin
    -- Identifier only
    insert into warehouse.sample (identifier, collection_identifier)
        values ('__SAMPLE__', null);

    -- Collection only
    with
    site as (
        insert into warehouse.site (identifier) values ('__SITE__')
        returning site_id as id
    ),
    individual as (
        insert into warehouse.individual (identifier) values ('__INDIVIDUAL__')
        returning individual_id as id
    )
    insert into warehouse.sample (identifier, collection_identifier)
        values (null, '__COLLECTION__');

    -- Both
    insert into warehouse.sample (identifier, collection_identifier)
        values ('__SAMPLE2__', '__COLLECTION2__');

    -- All nulls
    declare
        _constraint text;
    begin
        insert into warehouse.sample (identifier, collection_identifier)
            values (null, null);
        assert false, 'insert succeeded';
    exception
        when check_violation then
            get stacked diagnostics _constraint = CONSTRAINT_NAME;
            assert _constraint = 'sample_identifiers_not_null', 'wrong constraint';
    end;
end $$;

rollback;
