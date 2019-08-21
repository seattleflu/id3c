-- Deploy seattleflu/schema:warehouse/location to pg
-- requires: warehouse/schema
-- requires: postgis
-- requires: hstore
-- requires: citext

begin;

set local search_path to warehouse, public;

create table location (
    location_id integer primary key generated by default as identity,
    identifier citext not null,
    scale citext not null,
    hierarchy hstore,
    point geometry(point, 4326),
    polygon geometry(multipolygon, 4326),
    simplified_polygon geometry(multipolygon, 4326),
    details jsonb,

    -- Within a scale (e.g. state or country), identifiers must be unique.
    -- For smaller scales (e.g. city or county) where names are likely to
    -- be shared (e.g. Moscow), this constraint may necessitate
    -- suffixing/prefixing/namespacing of identifiers (e.g. Moscow, Russia
    -- or Moscow (Pennsylvania)).
    unique (scale, identifier),

    -- A location's point should be within its polygon to prevent display
    -- weirdness.  This also ensures geometric operations (e.g. covers or
    -- intersects) have a better chance of making sense.
    constraint location_point_must_be_in_polygon
        check (st_covers(polygon, point))
);

comment on table location is 'Hierarchical geospatial locations';
comment on column location.location_id is 'Internal id of this location';
comment on column location.identifier is 'External identifier by which this location is known; case-insensitive';
comment on column location.scale is 'Relative size or extent of this location (e.g. country, state, city); case-insensitive';
comment on column location.hierarchy is 'Set of key-value pairs describing where this location fits within a hierarchy';
comment on column location.point is 'Representative point geometry in WGS84 (EPSG:4326)';
comment on column location.polygon is 'Multi-polygon geometry in WGS84 (EPSG:4326)';
comment on column location.simplified_polygon is 'Multi-polygon geometry in WGS84 (EPSG:4326) with reduced complexity/accuracy, intended for cartographic use';
comment on column location.details is 'Additional information about this location which does not have a place in the relational schema';

-- Hierarchy containment (@>) queries are expected to be used for aggregation
create index location_hierarchy_idx on location using gist (hierarchy);

-- Geospatial indexes speed up geospatial functions and operators
create index location_point_idx on location using gist (point);
create index location_polygon_idx on location using spgist (polygon);
create index location_simplified_polygon_idx on location using spgist (simplified_polygon);

-- Index details document by default so containment queries on it are quick
create index location_details_idx on location using gin (details jsonb_path_ops);

commit;