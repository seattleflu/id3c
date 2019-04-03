-- Revert seattleflu/schema:warehouse/site from pg

begin;

drop table warehouse.site;

commit;
