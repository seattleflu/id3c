-- Revert seattleflu/schema:receiving/enrollment/processing-log from pg

begin;

alter table receiving.enrollment
    add column processed timestamp with time zone;

comment on column receiving.enrollment.processed is
    'When the document was loaded into the research schema, if not null.';

-- Keep the most recent timestamp from the log, if any
update receiving.enrollment
   set processed = (processing_log->(-1)->>'timestamp')::timestamp with time zone;

alter table receiving.enrollment drop column processing_log;

commit;
