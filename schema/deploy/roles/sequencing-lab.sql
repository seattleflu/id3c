-- Deploy seattleflu/schema:roles/sequencing-lab to pg
-- requires: receiving/sequence-read-set

begin;

create role "sequencing-lab";

grant connect on database :"DBNAME" to "sequencing-lab";
grant usage on schema receiving to "sequencing-lab";
grant insert (document) on receiving.sequence_read_set to "sequencing-lab";

comment on role "sequencing-lab" is
    'For reporting new sequencing results';

commit;
