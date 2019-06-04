-- Verify seattleflu/schema:warehouse/encounter/age on pg

begin;

do $$ begin
    -- Insert encounters with various ages
    with
    site as (
        insert into warehouse.site (identifier) values ('__SITE__')
        returning site_id as id
    ),
    individual as (
        insert into warehouse.individual (identifier) values ('__INDIVIDUAL__')
        returning individual_id as id
    )
    insert into warehouse.encounter (identifier, individual_id, site_id, encountered, age)
        values('__ENCOUNTER_1__', (select id from individual), (select id from site), now(), '1 year 2 months'),
              ('__ENCOUNTER_2__', (select id from individual), (select id from site), now(), '1 month'),
              ('__ENCOUNTER_3__', (select id from individual), (select id from site), now(), null);

    -- Assert extractions of intervals work as expected
    assert (select extract(year from age) from warehouse.encounter where identifier = '__ENCOUNTER_1__') = 1;
    assert (select extract(month from age) from warehouse.encounter where identifier = '__ENCOUNTER_1__') = 2;
    assert (select extract(year from age) from warehouse.encounter where identifier ='__ENCOUNTER_2__') = 0;
    assert (select extract(month from age) from warehouse.encounter where identifier ='__ENCOUNTER_2__') = 1;

end $$;

rollback;
