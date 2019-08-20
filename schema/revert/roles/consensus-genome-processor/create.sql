-- Revert seattleflu/schema:roles/consensus-genome-processor/create from pg

begin;

drop role "consensus-genome-processor";

commit;
