-- Deploy seattleflu/schema:staging/enrollment/document-constraint to pg
-- requires: staging/enrollment

begin;

alter table staging.enrollment
    add constraint enrollment_document_is_object
        check (json_typeof(document) = 'object')
;

commit;
