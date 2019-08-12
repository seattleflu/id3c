-- Revert seattleflu/schema:receiving/sequence-read-set from pg

begin;

set local role id3c;

drop table receiving.sequence_read_set;

commit;
