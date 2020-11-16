-- Deploy seattleflu/schema:receiving/redcap-det/indexes/processing-log to pg
-- requires: receiving/redcap-det

begin;

create index redcap_det_processing_log_idx
  on receiving.redcap_det
  using gin (processing_log jsonb_path_ops);

commit;
