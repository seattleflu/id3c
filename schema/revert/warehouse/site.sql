-- Revert seattleflu/schema:warehouse/site from pg

begin;

set local role id3c;

drop table warehouse.site;

commit;
