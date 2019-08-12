-- Revert seattleflu/schema:receiving/manifest from pg

begin;

set local role id3c;

drop table receiving.manifest;

commit;
