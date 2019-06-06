# REDCap

Move to REDCap for initial data capture instead of Audere + OneDrive.

Involves:

* Setting up REDCap so it will work for all stakeholders.

* Setting up data entry triggers (webhooks) to post data in real-time to
  endpoints for enrollments and manifest records.

* Converting enrollment data to FHIR?

## Motivations

1. Audere is going away, at least for prospective, kiosk- or clinic-based
   studies.  Maybe still around for at-home studies?

2. Centralize around a common core of survey questions and single enrollment
   repository with better visibility for groups doing enrollment.

3. Better pipeline for receiving specimen manifest records.

## Challenges

1. Should we push to map question/answers to a BioPortal or FHIR ontology?

2. How closely do we need to pay attention now to survey construction to avoid
   pitfalls later?  Probably closely.

## Prior art

* REDCap on FHIR — export FHIR documents from REDCap, paper and slides
  available, not sure about code.

* Dynamic data pulls (DDP) on FHIR — a way to have EMR records flow into REDCap
  (and then onto ID3C)

* FHIRCap?
