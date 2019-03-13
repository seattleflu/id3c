-- Deploy seattleflu/schema:receiving/enrollment/processing-log to pg
-- requires: receiving/enrollment

begin;

alter table receiving.enrollment
    add column processing_log jsonb not null default '[]'
        constraint enrollment_processing_log_is_array
            check (jsonb_typeof(processing_log) = 'array');

comment on column receiving.enrollment.processing_log is
    'Event log recording details of ETL into the warehouse';

create index enrollment_processing_log_idx on receiving.enrollment using gin (processing_log jsonb_path_ops);

update receiving.enrollment
   set processing_log = jsonb_build_array(jsonb_build_object('timestamp', processed))
 where processed is not null;

alter table receiving.enrollment drop column processed;

commit;
