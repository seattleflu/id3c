-- Revert seattleflu/schema:receiving/redcap-det/indexes/processing-log from pg

begin;

drop index receiving.redcap_det_processing_log_idx;

commit;
