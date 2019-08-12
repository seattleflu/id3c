-- Deploy seattleflu/schema:warehouse/encounter/age to pg
-- requires: warehouse/encounter

begin;

set local role id3c;

alter table warehouse.encounter
    add column age interval
    constraint encounter_age_only_precise_to_months 
        check(date_trunc('month', age) = age);

comment on column warehouse.encounter.age is
    'Age of individual at the time of encounter';

commit;
