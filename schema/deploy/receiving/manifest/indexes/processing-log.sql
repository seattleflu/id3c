-- Deploy seattleflu/schema:receiving/manifest/indexes/processing-log to pg
-- requires: receiving/manifest

begin;

create index manifest_processing_log_idx
  on receiving.manifest
  using gin (processing_log jsonb_path_ops);

commit;
