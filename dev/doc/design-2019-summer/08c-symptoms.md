# Symptoms

Pull out responses to what symptoms a participant has.

## Motivation

1. Remove need for digging into the JSON for queries.

## Challenges

1. Should this instead be a custom domain (text[] with a check constraint on
   values)?

2. Map all our terms to a common ontology, e.g. NCIT (see below), instead of
   using bespoke identifiers.

## Sketch of a schema

    symptom
      symptom_id
      identifier (e.g. ncit:C3038)
      name (e.g. fever)

    encounter_symptom
      encounter_id
      symptom_id

or maybe:

    create domain symptoms as text[]
      check (array['cough', 'fever', ...] @> value)

## Prior art

* [National Cancer Institute Thesaurus](https://bioportal.bioontology.org/ontologies/NCIT)
