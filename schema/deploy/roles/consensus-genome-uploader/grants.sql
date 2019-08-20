-- Deploy seattleflu/schema:roles/consensus-genome-uploader/grants to pg
-- requires: receiving/consensus-genome
-- requires: roles/consensus-genome-uploader/create

begin;

grant connect on database :"DBNAME" to "consensus-genome-uploader";

grant usage
   on schema receiving
   to "consensus-genome-uploader";

grant select (consensus_genome_id)
   on receiving.consensus_genome
   to "consensus-genome-uploader";

grant insert (document)
   on receiving.consensus_genome
   to "consensus-genome-uploader";

commit;
