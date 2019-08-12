-- Revert seattleflu/schema:receiving/enrollment/document-constraint from pg

begin;

set local role id3c;

alter table receiving.enrollment
    drop constraint enrollment_document_is_object
;

commit;
