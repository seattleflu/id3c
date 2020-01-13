-- Deploy seattleflu/schema:shipping/views to pg
-- requires: shipping/schema
-- requires: functions/array_distinct

-- Hello!  All shipping views are defined here.  Rework this change with Sqitch
-- to change a view definition or add new views.  This workflow helps keep
-- inter-view dependencies manageable.

begin;

-- This view is versioned as a hedge against future changes.  Changing this
-- view in place is fine as long as changes are backwards compatible.  Think of
-- the version number as the major part of a semantic versioning scheme.  If
-- there needs to be a lag between view development and consumers being
-- updated, copy the view definition into v2 and make changes there.

create or replace view shipping.presence_absence_result_v1 as

    select sample.identifier as sample,
           target.identifier as target,
           present,
           organism.lineage as organism

    from warehouse.sample
    join warehouse.presence_absence using (sample_id)
    join warehouse.target using (target_id)
    left join warehouse.organism using (organism_id)
    where target.control = false;

comment on view shipping.presence_absence_result_v1 is
    'View of warehoused presence-absence results for modeling and viz teams';

revoke all
    on shipping.presence_absence_result_v1
  from "incidence-modeler";

grant select
    on shipping.presence_absence_result_v1
    to "incidence-modeler";

commit;
