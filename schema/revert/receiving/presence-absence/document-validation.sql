-- Revert seattleflu/schema:receiving/presence-absence/document-validation from pg

begin;

alter table receiving.presence_absence
    drop constraint presence_absence_document_validates;

commit;
