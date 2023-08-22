-- Deploy seattleflu/schema:roles/reporter/revoke-select-on-receiving-sequence-read-set to pg

begin;

revoke select on receiving.sequence_read_set from reporter;

commit;
