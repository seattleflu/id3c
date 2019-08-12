-- Revert seattleflu/schema:receiving/presence-absence from pg

begin;

set local role id3c;

drop table receiving.presence_absence;

commit;
