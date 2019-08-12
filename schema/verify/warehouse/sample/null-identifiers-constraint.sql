-- Verify seattleflu/schema:warehouse/sample/null-identifiers-constraint on pg

begin;

set local role id3c;

do $$ begin
    -- Identifier only
    insert into warehouse.sample (identifier, collection_identifier, encounter_id)
        values ('__SAMPLE__', null, null);

    -- Collection and encounter
    with
    site as (
        insert into warehouse.site (identifier) values ('__SITE__')
        returning site_id as id
    ),
    individual as (
        insert into warehouse.individual (identifier) values ('__INDIVIDUAL__')
        returning individual_id as id
    ),
    encounter as (
        insert into warehouse.encounter (identifier, individual_id, site_id, encountered)
            select '__ENCOUNTER__', individual.id, site.id, now()
              from individual, site
        returning encounter_id as id
    )
    insert into warehouse.sample (identifier, collection_identifier, encounter_id)
        values (null, '__COLLECTION__', (select id from encounter));

    -- All nulls
    declare
        _constraint text;
    begin
        insert into warehouse.sample (identifier, collection_identifier, encounter_id)
            values (null, null, null);
        assert false, 'insert succeeded';
    exception
        when check_violation then
            get stacked diagnostics _constraint = CONSTRAINT_NAME;
            assert _constraint = 'sample_identifiers_not_null', 'wrong constraint';
    end;

    -- Collection without encounter
    declare
        _constraint text;
    begin
        insert into warehouse.sample (identifier, collection_identifier, encounter_id)
            values (null, '__COLLECTION2__', null);
        assert false, 'insert succeeded';
    exception
        when check_violation then
            get stacked diagnostics _constraint = CONSTRAINT_NAME;
            assert _constraint = 'sample_identifiers_not_null', 'wrong constraint';
    end;
end $$;

rollback;
