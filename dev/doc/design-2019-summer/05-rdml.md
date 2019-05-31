# RDML

Adopt [RDML](http://rdml.org/) as a standard for presence/absence data?

* TAQMAN panel
* Cepheid results
* Clinical testing?

## Motivations

1. Stop using a bespoke format internal to Samplify.

## Challenges

1. This is a clear choice for qPCR-based presence/absence results (with Cq
   values), but does RDML make sense for presence/absence results reported
   without a quantification?  Does the format allow for this?

2. We will need to either convince NWGC to send us the RDML, or we will need to
   transform from their JSON to RDML ourselves.
