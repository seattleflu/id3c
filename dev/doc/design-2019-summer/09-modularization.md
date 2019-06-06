# Modularization

Refactor parts of ID3C which are generic from parts which are specific to the
Seattle Flu Study, making a core component which is extended by customizations
and plugins.  This is a standard design approach and will probably be easier
to do sooner than later.

The common core (epiphyla/id3c?) would contain:

* base CLI
* base web API
* ETL framework
* sqitch project with warehouse schema and some of receiving (FHIR, RDML,
  sequence read sets, genomes, etc)

Extension packages (seattleflu/id3c?) would contain:

* custom ETL routines, loaded as command plugins (via setuptools entrypoints)
  and using id3c Python libraries

* custom database schema (receiving, shipping) as a sqitch project with
  cross-project dependencies (these are supported!)

* code to handle the "edges", the places where the core meets the outside world

## Motivation

1. Replace fauna and power the next generation of Nextstrain work.

2. Be a re-usable data system like Augur is a re-usable bioinformatics toolkit
   and Auspice is a re-usable visualization tool.

## Challenges

1. Adds complexity during development with an additional repo to touch.

2. After doing the work, there will be a one-time cutover/flag day where we go
   through the incantations to make it so in production.  This will involve
   things like making use of sqitch's log-only deploys to pick up new names for
   already-deployed changes.
