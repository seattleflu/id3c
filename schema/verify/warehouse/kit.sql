-- Verify seattleflu/schema:warehouse/kit on pg

begin;

select pg_catalog.has_table_privilege('warehouse.kit', 'select');

-- Check kit_references_not_null constraint is working 
do $$ begin
    -- Encounter reference only
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
    insert into warehouse.kit (identifier, encounter_id, rdt_sample_id, utm_sample_id)
        values ('__KIT1__', (select id from encounter), null, null);

    -- One sample reference only
    with
    sample as (
        insert into warehouse.sample (identifier) values ('__SAMPLE__')
        returning sample_id as id
    )
    insert into warehouse.kit (identifier, encounter_id, rdt_sample_id, utm_sample_id)
        values ('__KIT2__', null, (select id from sample), null);
    
    -- Two sample references without encounter
    with
    sample1 as (
        insert into warehouse.sample (identifier) values ('__SAMPLE1__')
        returning sample_id as id
    ),
    sample2 as (
        insert into warehouse.sample (identifier) values ('__SAMPLE2__')
        returning sample_id as id
    )
    insert into warehouse.kit (identifier, encounter_id, rdt_sample_id, utm_sample_id)
        values ('__KIT3__', null, (select id from sample1), (select id from sample2));

    -- All nulls
    declare
        _constraint text;
    begin
        insert into warehouse.kit (identifier, encounter_id, rdt_sample_id, utm_sample_id)
            values ('__KIT4__', null, null, null);
        assert false, 'insert succeeded';
    exception
        when check_violation then
            get stacked diagnostics _constraint = CONSTRAINT_NAME;
            assert _constraint = 'kit_references_not_null', 'wrong constraint';
    end;

end $$;

rollback;
