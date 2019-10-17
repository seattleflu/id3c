-- Revert seattleflu/schema:receiving/fhir from pg

begin;

drop table receiving.fhir;

commit;
