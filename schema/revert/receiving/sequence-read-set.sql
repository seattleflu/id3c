-- Revert seattleflu/schema:receiving/sequence-read-set from pg

begin;

drop table receiving.sequence_read_set;

commit;
