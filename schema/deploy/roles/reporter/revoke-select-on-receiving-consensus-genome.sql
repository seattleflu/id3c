-- Deploy seattleflu/schema:roles/reporter/revoke-select-on-receiving-consensus-genome to pg

begin;

revoke select on receiving.consensus_genome from reporter;

commit;
