-- Deploy seattleflu/schema:receiving/fhir/indexes/processing-log to pg
-- requires: receiving/fhir

begin;

create index fhir_processing_log_idx
  on receiving.fhir
  using gin (processing_log jsonb_path_ops);

commit;
