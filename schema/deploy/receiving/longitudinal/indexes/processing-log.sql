-- Deploy seattleflu/schema:receiving/longitudinal/indexes/processing-log to pg
-- requires: receiving/longitudinal

begin;

create index longitudinal_processing_log_idx
  on receiving.longitudinal
  using gin (processing_log jsonb_path_ops);

commit;
