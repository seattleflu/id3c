-- Revert seattleflu/schema:receiving/clinical from pg

begin;

drop table receiving.clinical;

commit;
