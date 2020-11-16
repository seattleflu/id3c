-- Deploy seattleflu/schema:receiving/presence-absence/indexes/processing-log to pg
-- requires: receiving/presence-absence

begin;

create index presence_absence_processing_log_idx
  on receiving.presence_absence
  using gin (processing_log jsonb_path_ops);

commit;
