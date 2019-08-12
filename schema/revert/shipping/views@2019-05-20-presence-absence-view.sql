-- Deploy seattleflu/schema:shipping/views to pg
-- requires: shipping/schema
-- requires: functions/array_distinct

-- Hello!  All shipping views are defined here.  Rework this change with Sqitch
-- to change a view definition or add new views.  This workflow helps keep
-- inter-view dependencies manageable.

begin;

set local role id3c;

-- This view is versioned as a hedge against future changes.  Changing this
-- view in place is fine as long as changes are backwards compatible.  Think of
-- the version number as the major part of a semantic versioning scheme.  If
-- there needs to be a lag between view development and consumers being
-- updated, copy the view definition into v2 and make changes there.

drop view shipping.incidence_model_observation_v1;

create view shipping.incidence_model_observation_v1 as

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
           case (encounter.details->'age'->>'ninetyOrAbove')::bool
             when true
             then 90
             else (encounter.details->'age'->>'value')::int
           end as age,

           age_bin_fine.range as age_range_fine,
           lower(age_bin_fine.range) as age_range_fine_lower,
           upper(age_bin_fine.range) as age_range_fine_upper,

           age_bin_coarse.range as age_range_coarse,
           lower(age_bin_coarse.range) as age_range_coarse_lower,
           upper(age_bin_coarse.range) as age_range_coarse_upper,

           -- XXX TODO: This will be pre-processed out of the JSON in the future.
           case (encounter.details->'locations'->'temp') is not null
             when true
             then encounter.details->'locations'->'temp'->>'region'
             else encounter.details->'locations'->'home'->>'region'
           end as residence_census_tract,

           -- XXX TODO: This will be pre-processed out of the JSON in the future.
           encounter.details->'locations'->'work'->>'region' as work_census_tract,

           encounter_responses.flu_shot,
           encounter_responses.symptoms,
           encounter_responses.race,
           encounter_responses.hispanic_or_latino

      from warehouse.encounter
      join warehouse.individual using (individual_id)
      join warehouse.site using (site_id)
      left join shipping.age_bin_fine on age_bin_fine.range @> cast(encounter.details->'age'->>'value' as int)
      left join shipping.age_bin_coarse on age_bin_coarse.range @> cast(encounter.details->'age'->>'value' as int),

      lateral (
          -- XXX TODO: The data in this subquery will be modeled better in the
          -- future and the verbosity of extracting data from the JSON details
          -- document will go away.
          --   -trs, 22 March 2019

          select -- XXX FIXME: Remove use of nullif() when we're no longer
                 -- dealing with raw response values.
                 nullif(responses."FluShot"[1], 'doNotKnow')::bool as flu_shot,

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

commit;
