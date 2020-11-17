-- Deploy seattleflu/schema:receiving/clinical/indexes/processing-log to pg
-- requires: receiving/clinical

begin;

create index clinical_processing_log_idx
  on receiving.clinical
  using gin (processing_log jsonb_path_ops);

commit;
