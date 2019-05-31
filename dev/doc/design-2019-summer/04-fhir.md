# FHIR

Adopt [FHIR R4](http://www.hl7.org/implement/standards/fhir/) as a standard for
enrollment data.

## Motivation

1. Simpler to focus on a single standard where specs are clear.

2. Decouples ID3C from the peculiarities of the Seattle Flu Study (towards the
   vision of a more general data system).

3. Promise of greater interoperability in the future from other data providers
   (HealthKit/ResearchKit/ResearchStack, EPIC and other EMRs, …).

## Challenges

1. FHIR is complex.
2. It may be hard to avoid special-casing our needs.

## Work

1. Writing a conversion from Audere's documents to FHIR (or get Audere to do this?)
2. Writing a conversion from REDCap surveys to FHIR (or using existing tools?)
3. Writing a conversion from clinical records to FHIR (or pulling these directly from the EMR?)
4. Write a _single_ ETL routine to handle all these sources of FHIR documents

FHIR resources we'd primarily use would be:

* Patient (sex)
* Encounter (age, site, place)
* Questionnaire

## Prior art

* [FHIRbase](https://www.health-samurai.io/fhirbase)
* REDCap on FHIR — export FHIR documents from REDCap, paper and slides
  available, not sure about code.
* Dynamic data pulls (DDP) on FHIR — a way to have EMR records flow into REDCap
  (and then onto ID3C)
* FHIRCap?
