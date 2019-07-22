-- Revert seattleflu/schema:roles/consensus-genome-uploader/grants from pg

begin;

revoke insert (document)
    on receiving.consensus_genome
  from "consensus-genome-uploader";

revoke select (consensus_genome_id)
    on receiving.consensus_genome
  from "consensus-genome-uploader";

revoke usage
    on schema receiving
  from "consensus-genome-uploader";

revoke connect on database :"DBNAME" from "consensus-genome-uploader";

commit;
