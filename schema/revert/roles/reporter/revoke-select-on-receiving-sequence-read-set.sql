-- Revert seattleflu/schema:roles/reporter/revoke-select-on-receiving-sequence-read-set from pg

begin;

grant select on receiving.sequence_read_set to reporter;

commit;
