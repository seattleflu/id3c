-- Revert seattleflu/schema:staging/enrollment/document-constraint from pg

begin;

alter table staging.enrollment
    drop constraint enrollment_document_is_object
;

commit;
