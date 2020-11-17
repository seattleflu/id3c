-- Revert seattleflu/schema:receiving/manifest/indexes/processing-log from pg

begin;

drop index receiving.manifest_processing_log_idx;

commit;
