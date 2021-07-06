-- Revert seattleflu/schema:warehouse/identifier-set/use from pg

begin;

alter table warehouse.identifier_set
    drop constraint identifier_set_use_fkey;

alter table warehouse.identifier_set
    drop column use;

commit;
