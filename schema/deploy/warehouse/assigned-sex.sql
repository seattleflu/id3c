-- Deploy seattleflu/schema:warehouse/assigned-sex to pg
-- requires: warehouse/schema

begin;

create domain warehouse.assigned_sex as text
    constraint assigned_sex_value check (
        value in ('male', 'female', 'other'));

comment on domain warehouse.assigned_sex is 'Administrative sex assigned at birth <http://www.hl7.org/implement/standards/fhir/codesystem-administrative-gender.html>';

commit;
