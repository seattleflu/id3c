-- Deploy seattleflu/schema:roles/consensus-genome-uploader/create to pg

begin;

create role "consensus-genome-uploader";

comment on role "consensus-genome-uploader" is 'For adding new consensus genome records';

commit;
