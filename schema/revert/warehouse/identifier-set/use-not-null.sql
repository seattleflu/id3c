-- Revert seattleflu/schema:warehouse/identifier-set/use-not-null from pg

begin;

alter table warehouse.identifier_set alter column use drop not null;

commit;
