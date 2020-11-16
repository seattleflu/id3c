-- Revert seattleflu/schema:receiving/fhir/indexes/processing-log from pg

begin;

drop index receiving.fhir_processing_log_idx;

commit;
