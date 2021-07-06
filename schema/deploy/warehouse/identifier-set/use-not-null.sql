-- Deploy seattleflu/schema:warehouse/identifier-set/use-not-null to pg
-- requires: warehouse/identifier-set/use

begin;

alter table warehouse.identifier_set alter column use set not null;

commit;
