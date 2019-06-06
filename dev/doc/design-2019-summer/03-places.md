# Places

A table of physical places of hierarchical scale, e.g. census tract →
neighborhood → community reporting area (CRA) → ….

Different instances of ID3C could contain different sets of places.

## Motivation

1. Modeling of residence/workplace identifiers so we can identify members of
   the same.

2. Allow ad-hoc geographic visualizations and analyses to happen within the
   database via SQL and/or Metabase.

3. Replace [seattleflu/seattle-geojson](https://github.com/seattleflu/seattle-geojson)
   as primary source of geographic shapes for modeling and visualization
   efforts.

5. Provide attachment point for metadata about a place, like a preferred
   representative point (defaulting to centroid) for a polygon, and other
   arbitrary details.

5. Replace many use cases for augur `lat_longs.tsv` files?

## Challenges

1. The data is graph-y, but we're in a relational model.

   The standard solution is to support a single parent/child relationship which
   is walked using "recursive" (actually iterative) queries in SQL and encoded
   into a view.

   We probably don't want to support a full graph with multiple potential paths
   for how one place relates to another.

   Another solution might be to pre-compute containment queries across all
   places and infer hierarchy instead of encoding it into relationships.

2. Model as a single table of places with different sets of scales or as one
   table per scale?

   This has implications for our ETL process.  Maybe "residence" wants to be
   modeled directly as a shape?

3. How to support consistent querying if the scale of an encounter's place is
   not enforced by schema?

## Sketch of potential schema

For places themselves:

    place
      place_id
      identifier
      place_scale_id
      parent_place_id
      geometry (polygon, nullable?)
      representative_point (point, default to centroid)
      details (json)

    place_scale
      place_scale_id
      name (e.g. US Census tract 2016, neighborhood, community reporting area,
            …, state/division/province, country, region, continent)
      details

For linking encounters to places:

    encounter
      residence_id
      workplace_id

    residence
      residence_id
      identifier (e.g. hashed addressed)
      place_id

    workplace
      workplace_id
      identifier (e.g. hashed addressed)
      place_id

It would also be good to link sites to places:

    site
      place_id

## Prior art and data

* [FHIR Location resources](http://www.hl7.org/implement/standards/fhir/location.html)
* [Who's On First](https://whosonfirst.org)
* [Community reporting areas (CRAs)](http://data-seattlecitygis.opendata.arcgis.com/datasets/community-reporting-areas)
