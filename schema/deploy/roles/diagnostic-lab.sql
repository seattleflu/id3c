-- Deploy seattleflu/schema:roles/diagnostic-lab to pg
-- requires: receiving/presence-absence

begin;

create role "diagnostic-lab";

grant connect on database :"DBNAME" to "diagnostic-lab";
grant usage on schema receiving to "diagnostic-lab";
grant insert (document) on receiving.presence_absence to "diagnostic-lab";

comment on role "diagnostic-lab" is
    'For reporting new diagnostic results';

commit;
