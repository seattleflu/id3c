-- Revert seattleflu/schema:roles/reporter/revoke-select-on-receiving-consensus-genome from pg

begin;

grant select on receiving.consensus_genome to reporter;

commit;
