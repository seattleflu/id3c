# Race and Ethnicity

Pull out responses about race and ethnicity.

## Motivation

1. Remove need for digging into the JSON for queries.

## Challenges

1. Should these instead be a custom domain (text[] with a check constraint on
   values)?

## Sketch of a schema

For race and ethnicity (hispanic/latino):

    race
      race_id
      name
      snomedct_term

    encounter_race
      encounter_id
      race_id

    encounter
      hispanic_or_latino (bool) ???

## Prior art

* [FHIR Race](https://www.hl7.org/fhir/v3/Race/cs.html)
* [FHIR Ethnicity](https://www.hl7.org/fhir/v3/Ethnicity/cs.html)
