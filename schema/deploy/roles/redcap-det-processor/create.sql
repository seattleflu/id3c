-- Deploy seattleflu/schema:roles/redcap-det-processor/create to pg

begin;

create role "redcap-det-processor";

comment on role "redcap-det-processor" is $$For REDCap DET ETL routines$$;

commit;
