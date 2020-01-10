-- Deploy seattleflu/schema:roles/fhir-processor/create to pg

begin;

create role "fhir-processor";

comment on role "fhir-processor" is $$For FHIR ETL routines$$;

commit;
