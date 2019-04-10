-- Revert seattleflu/schema:receiving/presence-absence from pg

begin;

drop table receiving.presence_absence;

commit;
