-- Revert seattleflu/schema:warehouse/sample/null-identifiers-constraint from pg

begin;

alter table warehouse.sample
    drop constraint sample_identifiers_not_null,
    alter column identifier set not null;

commit;
