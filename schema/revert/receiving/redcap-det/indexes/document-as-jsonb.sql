-- Revert seattleflu/schema:receiving/redcap-det/indexes/document-as-jsonb from pg
-- requires: receiving/redcap-det

begin;

drop index receiving.redcap_det_document_as_jsonb_idx;

commit;
