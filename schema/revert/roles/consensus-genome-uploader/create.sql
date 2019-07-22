-- Revert seattleflu/schema:roles/consensus-genome-uploader/create from pg

begin;

drop role "consensus-genome-uploader";

commit;
