-- Deploy seattleflu/schema:receiving/enrollment/document-constraint to pg
-- requires: receiving/enrollment

begin;

set local role id3c;

alter table receiving.enrollment
    add constraint enrollment_document_is_object
        check (json_typeof(document) = 'object')
;

commit;
