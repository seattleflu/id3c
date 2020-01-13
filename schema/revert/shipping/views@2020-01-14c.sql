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

create or replace view shipping.incidence_model_observation_v1 as

    select encounter.identifier as encounter,

           (encountered at time zone 'US/Pacific')::date as encountered_date,
           to_char((encountered at time zone 'US/Pacific')::date, 'IYYY-"W"IW') as encountered_week,

           site.details->>'type' as site_type,

           individual.identifier as individual,
           individual.sex,

           -- Reporting 90 is more accurate than reporting nothing, and it will
           -- never be a real value in our dataset.
           --
           -- XXX TODO: This will be pre-processed out of the JSON in the future.
           ceiling(age_in_years(age))::int as age,

           age_bin_fine.range as age_range_fine,
           lower(age_bin_fine.range) as age_range_fine_lower,
           upper(age_bin_fine.range) as age_range_fine_upper,

           age_bin_coarse.range as age_range_coarse,
           lower(age_bin_coarse.range) as age_range_coarse_lower,
           upper(age_bin_coarse.range) as age_range_coarse_upper,

           residence_census_tract,
           work_census_tract,

           encounter_responses.flu_shot,
           encounter_responses.symptoms,
           encounter_responses.race,
           encounter_responses.hispanic_or_latino,

           sample.identifier as sample

      from warehouse.encounter
      join warehouse.individual using (individual_id)
      join warehouse.site using (site_id)
      left join warehouse.sample using (encounter_id)
      left join shipping.age_bin_fine on age_bin_fine.range @> ceiling(age_in_years(age))::int
      left join shipping.age_bin_coarse on age_bin_coarse.range @> ceiling(age_in_years(age))::int
      left join (
          select encounter_id, hierarchy->'tract' as residence_census_tract
          from warehouse.encounter_location
          left join warehouse.location using (location_id)
          where relation = 'residence'
          or relation = 'lodging'
        ) as residence using (encounter_id)
      left join (
          select encounter_id, hierarchy->'tract' as work_census_tract
          from warehouse.encounter_location
          left join warehouse.location using (location_id)
          where relation = 'workplace'
        ) as workplace using (encounter_id),

      lateral (
          -- XXX TODO: The data in this subquery will be modeled better in the
          -- future and the verbosity of extracting data from the JSON details
          -- document will go away.
          --   -trs, 22 March 2019

          select -- XXX FIXME: Remove use of nullif() when we're no longer
                 -- dealing with raw response values.
                 nullif(nullif(responses."FluShot"[1], 'doNotKnow'), 'dontKnow')::bool as flu_shot,

                 -- XXX FIXME: Remove duplicate value collapsing when we're no
                 -- longer affected by this known Audere data quality issue.
                 array_distinct(responses."Symptoms") as symptoms,
                 array_distinct(responses."Race") as race,

                 -- XXX FIXME: Remove use of nullif() when we're no longer
                 -- dealing with raw response values.
                 nullif(responses."HispanicLatino"[1], 'preferNotToSay')::bool as hispanic_or_latino

            from jsonb_to_record(encounter.details->'responses')
              as responses (
                  "FluShot" text[],
                  "Symptoms" text[],
                  "Race" text[],
                  "HispanicLatino" text[]))
        as encounter_responses

     order by encountered;

comment on view shipping.incidence_model_observation_v1 is
    'View of warehoused encounters and important questionnaire responses for modeling and viz teams';

revoke all
    on shipping.incidence_model_observation_v1
  from "incidence-modeler";

grant select
   on shipping.incidence_model_observation_v1
   to "incidence-modeler";

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


create or replace view shipping.incidence_model_observation_v2 as

    select encounter.identifier as encounter,

           (encountered at time zone 'US/Pacific')::date as encountered_date,
           to_char((encountered at time zone 'US/Pacific')::date, 'IYYY-"W"IW') as encountered_week,

           site.identifier as site,
           site.details->>'type' as site_type,

           individual.identifier as individual,
           individual.sex,

           age_in_years(age) as age,

           age_bin_fine_v2.range as age_range_fine,
           age_in_years(lower(age_bin_fine_v2.range)) as age_range_fine_lower,
           age_in_years(upper(age_bin_fine_v2.range)) as age_range_fine_upper,

           age_bin_coarse_v2.range as age_range_coarse,
           age_in_years(lower(age_bin_coarse_v2.range)) as age_range_coarse_lower,
           age_in_years(upper(age_bin_coarse_v2.range)) as age_range_coarse_upper,

           residence_census_tract,
           work_census_tract,

           encounter_responses.flu_shot,
           encounter_responses.symptoms,
           encounter_responses.race,
           encounter_responses.hispanic_or_latino,

           sample.identifier as sample

      from warehouse.encounter
      join warehouse.individual using (individual_id)
      join warehouse.site using (site_id)
      left join warehouse.sample using (encounter_id)
      left join shipping.age_bin_fine_v2 on age_bin_fine_v2.range @> age
      left join shipping.age_bin_coarse_v2 on age_bin_coarse_v2.range @> age
      left join (
          select encounter_id, hierarchy->'tract' as residence_census_tract
          from warehouse.encounter_location
          left join warehouse.location using (location_id)
          where relation = 'residence'
          or relation = 'lodging'
        ) as residence using (encounter_id)
      left join (
          select encounter_id, hierarchy->'tract' as work_census_tract
          from warehouse.encounter_location
          left join warehouse.location using (location_id)
          where relation = 'workplace'
        ) as workplace using (encounter_id),

      lateral (
          -- XXX TODO: The data in this subquery will be modeled better in the
          -- future and the verbosity of extracting data from the JSON details
          -- document will go away.
          --   -trs, 22 March 2019

          select -- XXX FIXME: Remove use of nullif() when we're no longer
                 -- dealing with raw response values.
                 nullif(nullif(responses."FluShot"[1], 'doNotKnow'), 'dontKnow')::bool as flu_shot,

                 -- XXX FIXME: Remove duplicate value collapsing when we're no
                 -- longer affected by this known Audere data quality issue.
                 array_distinct(responses."Symptoms") as symptoms,
                 array_distinct(responses."Race") as race,

                 -- XXX FIXME: Remove use of nullif() when we're no longer
                 -- dealing with raw response values.
                 nullif(responses."HispanicLatino"[1], 'preferNotToSay')::bool as hispanic_or_latino

            from jsonb_to_record(encounter.details->'responses')
              as responses (
                  "FluShot" text[],
                  "Symptoms" text[],
                  "Race" text[],
                  "HispanicLatino" text[]))
        as encounter_responses

     order by encountered;

comment on view shipping.incidence_model_observation_v2 is
    'Version 2 of view of warehoused encounters and important questionnaire responses for modeling and viz teams';

revoke all
    on shipping.incidence_model_observation_v2
  from "incidence-modeler";

grant select
   on shipping.incidence_model_observation_v2
   to "incidence-modeler";


create or replace view shipping.observation_with_presence_absence_result_v1 as

    select target,
           present,
           present::int as presence,
           observation.*,
           organism
      from shipping.incidence_model_observation_v2 as observation
      join shipping.presence_absence_result_v1 using (sample)
      order by site, encounter, sample, target;

comment on view shipping.observation_with_presence_absence_result_v1 is
  'Joined view of shipping.incidence_model_observation_v2 and shipping.presence_absence_result_v1';


create or replace view shipping.incidence_model_observation_v3 as

    select encounter.identifier as encounter,

           to_char((encountered at time zone 'US/Pacific')::date, 'IYYY-"W"IW') as encountered_week,

           site.details->>'type' as site_type,

           individual.identifier as individual,
           individual.sex,

           age_bin_fine_v2.range as age_range_fine,
           age_in_years(lower(age_bin_fine_v2.range)) as age_range_fine_lower,
           age_in_years(upper(age_bin_fine_v2.range)) as age_range_fine_upper,

           age_bin_coarse_v2.range as age_range_coarse,
           age_in_years(lower(age_bin_coarse_v2.range)) as age_range_coarse_lower,
           age_in_years(upper(age_bin_coarse_v2.range)) as age_range_coarse_upper,

           residence_census_tract,

           encounter_responses.flu_shot,
           encounter_responses.symptoms,

           sample.identifier as sample

      from warehouse.encounter
      join warehouse.individual using (individual_id)
      join warehouse.site using (site_id)
      left join warehouse.sample using (encounter_id)
      left join shipping.age_bin_fine_v2 on age_bin_fine_v2.range @> age
      left join shipping.age_bin_coarse_v2 on age_bin_coarse_v2.range @> age
      left join (
          select encounter_id, hierarchy->'tract' as residence_census_tract
          from warehouse.encounter_location
          left join warehouse.location using (location_id)
          where relation = 'residence'
          or relation = 'lodging'
        ) as residence using (encounter_id),

      lateral (
          -- XXX TODO: The data in this subquery will be modeled better in the
          -- future and the verbosity of extracting data from the JSON details
          -- document will go away.
          --   -trs, 22 March 2019

          select -- XXX FIXME: Remove use of nullif() when we're no longer
                 -- dealing with raw response values.
                 nullif(nullif(responses."FluShot"[1], 'doNotKnow'), 'dontKnow')::bool as flu_shot,

                 -- XXX FIXME: Remove duplicate value collapsing when we're no
                 -- longer affected by this known Audere data quality issue.
                 array_distinct(responses."Symptoms") as symptoms

            from jsonb_to_record(encounter.details->'responses')
              as responses (
                  "FluShot" text[],
                  "Symptoms" text[]))
        as encounter_responses

     order by encountered;

comment on view shipping.incidence_model_observation_v3 is
    'Version 3 of view of warehoused encounters and important questionnaire responses for modeling and viz teams';

revoke all
    on shipping.incidence_model_observation_v3
  from "incidence-modeler";

grant select
   on shipping.incidence_model_observation_v3
   to "incidence-modeler";


create or replace view shipping.observation_with_presence_absence_result_v2 as

    select target,
           present,
           present::int as presence,
           observation.*,
           organism
      from shipping.incidence_model_observation_v3 as observation
      join shipping.presence_absence_result_v1 using (sample)
      order by site_type, encounter, sample, target;

comment on view shipping.observation_with_presence_absence_result_v2 is
  'Joined view of shipping.incidence_model_observation_v3 and shipping.presence_absence_result_v1';

commit;
