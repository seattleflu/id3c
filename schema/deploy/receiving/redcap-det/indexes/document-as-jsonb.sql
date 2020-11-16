-- Deploy seattleflu/schema:receiving/redcap-det/indexes/document-as-jsonb to pg
-- requires: receiving/redcap-det

begin;

create index redcap_det_document_as_jsonb_idx
  on receiving.redcap_det
  using gin ((document::jsonb) jsonb_path_ops);

commit;
