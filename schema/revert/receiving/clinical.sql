-- Revert seattleflu/schema:receiving/clinical from pg

begin;

set local role id3c;

drop table receiving.clinical;

commit;
