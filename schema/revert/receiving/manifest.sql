-- Revert seattleflu/schema:receiving/manifest from pg

begin;

drop table receiving.manifest;

commit;
