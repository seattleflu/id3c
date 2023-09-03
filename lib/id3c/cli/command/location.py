"""
Import or lookup geographic locations.

Locations in ID3C are hierarchical geospatial points or areas.  They are
uniquely identified by their tuple (scale, identifier) and may have the
following geometries attached to them:

\b
* point
* polygon, for areas
* simplified polygon, for cartographic (mapping) purposes

Points will be automatically calculated if only a polygon is provided.

Hierarchies of locations are defined by overlapping sets of key=>value
pairs.  For example, the following locations might be used to represent US
geography.

\b
  # scale     identifier   hierarchy
  ('country', 'US',        'country => US')
  ('state',   'WA',        'country => US, state => WA')
  ('city',    'Seattle',   'country => US, state => WA, city => Seattle')

Hierarchies must be fully-specified for all locations for containment testing
to work as expected.
"""
import sys
import click
import fiona
import fiona.crs
import json
import logging
import pandas as pd
import id3c.db as db
from io import StringIO
from psycopg2.sql import SQL
from textwrap import dedent
from typing import Optional, Tuple
from id3c.cli import cli
from id3c.db.datatypes import Json
from id3c.db.types import MinimalLocationRecord
from id3c.db.session import DatabaseSession
from id3c.cli.io.pandas import (
    load_input_from_file_or_stdin,
)

LOG = logging.getLogger(__name__)


def fiona_path(path):
    if path.lower().endswith(".zip"):
        return f"zip://{path}"
    else:
        return path


@cli.group("location", help = __doc__)
def location():
    pass


@location.command("import")
@click.argument("features_path",
    metavar = "<features.{geojson,shp,csv,tsv}>",
    type = fiona_path)

@click.option("--scale",
    metavar = "<scale>",
    help = "Relative size or extent of all locations (e.g. country, state, city); if this is provided, it overrides --scale-from")

@click.option("--scale-from",
    metavar = "<property>",
    default = "scale",
    help = "Use the given property name as the scale for each location; default is \"scale\"")

@click.option("--identifier-from",
    metavar = "<property>",
    help = "Use the given property name as the unique location identifier; default for GeoJSON is the feature's id (note that this is a different thing than the feature's \"id\" property)")

@click.option("--hierarchy",
    metavar = "<hierarchy>",
    help = "Comma-separated string of key=>value pairs describing where these locations fit within a hierarchy; if this is provided, it overrides --hierarchy-from")

@click.option("--hierarchy-by-feature",
    metavar = "<hierarchy.csv>",
    type = click.File("r"),
    help = "CSV file of hierachy to include for features that must include `feature_identifier` column. All other column headers are assumed to be hierarchy keys; " +
           "if provided, it overrides --hierarchy-from and can override --hierarchy if same key provided")

@click.option("--hierarchy-from",
    metavar = "<property>",
    help = "Use the given property name as the hierarchy for each location; default is \"hierarchy\"")

@click.option("--point-from",
    metavar = "<lon> <lat>",
    nargs = 2,
    help = "Construct a point for locations using two property names providing the (lon, lat) in EPSG 4326")

@click.option("--simplified-polygons", "simplified_polygons_path",
    metavar = "<simplified.{geojson,shp}>",
    type = fiona_path,
    help = "Additional geometry file of simplified polygon features intended for cartographic use; must use the same identifier as the primary feature")

@click.option("--if-exists", "if_exists_action",
    type = click.Choice(["error", "update", "skip"]),
    default = "error",
    help = "What to do when a location with the same (scale, identifier) already exists; default is to error.")

def import_(features_path,
            scale,
            scale_from,
            hierarchy,
            hierarchy_by_feature,
            hierarchy_from,
            identifier_from,
            point_from,
            simplified_polygons_path,
            if_exists_action):
    """
    Import locations from GeoJSON, Shapefile, or tabular delimited text (CSV/TSV).

    Run `id3c location --help` for general information about locations.

    Many common geospatial formats are supported via Fiona
    <https://fiona.readthedocs.io/en/latest/README.html>, including:

    \b
    * GeoJSON (top-level type must be ``FeatureCollection``)
    * Shapefiles (*.shp with sidecar *.dbf file; may be zipped)
    * tabular (CSV or TSV, delimiter auto-detected, no geometry construction)

    If a tabular data file is provided, at least one of --point-from or
    --simplified-polygons should be used to provide a geometry.

    Each location's (scale, identifier) pair is automatically added to its
    hierarchy during import.  This is a non-configurable convention that could
    be made optional in the future.
    """
    # First, a handful of accessor and transformation functions to make the
    # code that follows more readable.  These functions make use of the command
    # options.
    def identifier(feature):
        if identifier_from:
            return feature["properties"].pop(identifier_from)
        else:
            return feature["id"]

    def geometry_type(feature):
        return feature["geometry"]["type"] if feature["geometry"] else None

    def point(feature):
        if point_from:
            return {
                "type": "Point",
                "coordinates": (
                    float(feature["properties"].pop(point_from[0])),
                    float(feature["properties"].pop(point_from[1])),
                )
            }
        elif geometry_type(feature) == "Point":
            return feature["geometry"]
        else:
            return None

    def polygon(feature):
        if geometry_type(feature) in {"Polygon", "MultiPolygon"}:
            return feature["geometry"]
        else:
            return None

    def get_hierarchy(feature, feature_identifier):
        if hierarchy_df is None:
            return hierarchy or feature["properties"].pop(hierarchy_from, None)

        hierarchy_list = hierarchy_df.loc[hierarchy_df['feature_identifier'] == feature_identifier].to_dict('record')
        hierarchies = ''
        if hierarchy_list:
            # This is assuming only one row has matching feature_identifier
            hierarchy_map = hierarchy_list[0]
            hierarchy_map.pop('feature_identifier')
            for key, val in hierarchy_map.items():
                if pd.notna(val):
                    hierarchies += (key + '=>' + val + ',')

        return hierarchies + hierarchy


    # Technically PostGIS' SRIDs don't have to match the EPSG id, but in my
    # experience, they always do in practice.  If that doesn't hold true in the
    # future, then a lookup of (auth_name, auth_id) in spatial_ref_sys table
    # will be needed to map to srid.
    #   -trs, 2 Dec 2019

    def as_location(feature):
        feature_identifier = identifier(feature)
        return {
            "scale": scale or feature["properties"].pop(scale_from),
            "identifier": feature_identifier,
            "hierarchy": get_hierarchy(feature, feature_identifier),
            "point": point(feature),
            "polygon": polygon(feature),
            "srid": feature["crs"]["EPSG"],
            "details": feature["properties"],
        }

    def as_simplified(feature):
        return {
            "identifier": identifier(feature),
            "polygon": polygon(feature),
            "srid": feature["crs"]["EPSG"],
        }

    # Now, read in the data files and convert to our internal structure.
    LOG.info(f"Reading features from «{features_path}»")

    hierarchy_df = None
    if hierarchy_by_feature:
        hierarchy_df = pd.read_csv(hierarchy_by_feature, dtype=str)
        if "feature_identifier" not in hierarchy_df.columns:
            raise Exception("hierarchy_by_feature CSV must include 'feature_identifier' column")
        duplicated_feature_identifier = hierarchy_df["feature_identifier"].duplicated(keep=False)
        duplicates = hierarchy_df["feature_identifier"][duplicated_feature_identifier]
        dup_identifiers = list(duplicates.unique())
        assert len(dup_identifiers) == 0, f"Found duplicate feature_identifiers: {dup_identifiers}"

    locations = list(map(as_location, parse_features(features_path)))

    if simplified_polygons_path:
        LOG.info(f"Reading simplified polygons from «{simplified_polygons_path}»")
        simplified_polygons = list(map(as_simplified, parse_features(simplified_polygons_path)))
    else:
        simplified_polygons = []

    # Finally, do the updates in the database
    db = DatabaseSession()

    try:
        LOG.info(f"Importing locations")

        insert = SQL("""
            with new_location as (
                select
                    scale,
                    identifier,
                    coalesce(nested_within.hierarchy, '') || coalesce(lower(hierarchy)::hstore, '') || hstore(lower(scale), lower(identifier)) as hierarchy,
                    st_transform(st_setsrid(st_geomfromgeojson(point), location.srid), 4326) as point,
                    st_transform(st_setsrid(st_multi(st_geomfromgeojson(location.polygon)), location.srid), 4326) as polygon,
                    st_transform(st_setsrid(st_multi(st_geomfromgeojson(simplified.polygon)), simplified.srid), 4326) as simplified_polygon,
                    details
                from jsonb_to_recordset(%s)
                    as location ( scale text
                                , identifier text
                                , hierarchy text
                                , point text
                                , polygon text
                                , srid integer
                                , details jsonb
                                )
                left join jsonb_to_recordset(%s)
                    as simplified ( identifier text
                                  , polygon text
                                  , srid integer
                                  )
                    using (identifier)
                left join lateral (
                        select hstore_agg(hierarchy)
                        from warehouse.location as containing
                        where st_within(location.point, containing.polygon))
                    as nested_within
                    on true
            ),
            inserted as (
                insert into warehouse.location (scale, identifier, hierarchy, point, polygon, simplified_polygon, details)
                table new_location
                {on_conflict}
                returning scale, identifier
            )
            select
                count(*) filter (where inserted is not null),
                count(*) filter (where inserted is not null and point is not null) as with_point,
                count(*) filter (where inserted is not null and polygon is not null) as with_polygon,
                count(*) filter (where inserted is not null and simplified_polygon is not null) as with_simplified_polygon,
                count(*) - count(*) filter (where inserted is not null) as skipped
            from new_location
            left join inserted using (scale, identifier)
        """)

        on_conflict = {
            "error": SQL(""),
            "update": SQL("""
                on conflict (scale, identifier) do update
                    set hierarchy           = EXCLUDED.hierarchy,
                        point               = EXCLUDED.point,
                        polygon             = EXCLUDED.polygon,
                        simplified_polygon  = EXCLUDED.simplified_polygon,
                        details             = EXCLUDED.details
            """),
            "skip": SQL("on conflict (scale, identifier) do nothing"),
        }

        imported = db.fetch_row(
            insert.format(on_conflict = on_conflict[if_exists_action]),
                (Json(locations), Json(simplified_polygons)))

        LOG.info(dedent(f"""\
            Imported {imported.count:,} locations
              {imported.with_point:,} with a point
              {imported.with_polygon:,} with a polygon
              {imported.with_simplified_polygon:,} with a simplified polygon
              {imported.skipped:,} skipped
            """))
        LOG.info("Committing all changes")
        db.commit()

    except:
        LOG.info("Rolling back all changes; the database will not be modified")
        db.rollback()
        raise


def parse_features(path):
    """
    Parse a collection of features (e.g. spatial records) from a file *path*.

    Many common geospatial formats are supported via Fiona
    <https://fiona.readthedocs.io/en/latest/README.html>, including:

    * GeoJSON (top-level type must be ``FeatureCollection``)
    * Shapefiles (*.shp with sidecar *.dbf file; may be zipped)
    * tabular (CSV or TSV, delimiter auto-detected, no geometry construction)

    If a coordinate reference system is not defined, EPSG:4326 is assumed and a
    warning issued.  If the CRS is an unaltered EPSG reference, features will
    be re-projected during import to EPSG:4326.  Otherwise, an
    :class:`Exception` is raised.

    Returns a list of dictionaries conforming to the GeoJSON ``Feature`` spec.
    Each dictionary also includes an additional top-level ``crs`` key
    containing with the EPSG spatial reference identifier of the collection.
    """
    collection = fiona.open(path)

    crs = collection.crs or {}

    if not crs:
        LOG.warning(f"No CRS defined.  EPSG:4326 will be assumed, but this may result in bad geometries.")
        crs = {"EPSG": 4326}

    elif crs.keys() == {"init",} and crs["init"].upper().startswith("EPSG:"):
        LOG.info(f"CRS is «{fiona.crs.to_string(crs)}»; will re-project to EPSG:4326.")
        crs = {"EPSG": int(crs["init"][5:])}

    else:
        raise Exception(f"Unsupported CRS «{fiona.crs.to_string(crs)}»")

    return [{**feature, "crs": crs} for feature in collection]


@location.command("lookup")
@click.argument("filename",
    metavar = "<filename.{csv,tsv,xls,xlsx}>",
    type = click.File("r"))

@click.option("--scale",
    metavar = "<scale>",
    default = "tract",
    show_default = True,
    help = "Relative size or extent of all locations (e.g. tract, PUMA) to lookup")

@click.option("--lat-column",
    metavar = "<column>",
    default = "lat",
    show_default = True,
    help = "Column name of latitude")

@click.option("--lng-column",
    metavar = "<column>",
    default = "lng",
    show_default = True,
    help = "Column name of longitude")

@click.option("--drop-latlng-columns",
    is_flag = True,
    help = "Remove input lat/lng columns from the output")

def lookup(filename: click.File,
           scale: str,
           lat_column: str,
           lng_column: str,
           drop_latlng_columns: bool):
    """
    Lookup locations containing a given latitude and longitude.

    <filename.{csv,tsv,xls,xlsx}> accepts `-` as a special file that refers
    to stdin, assuming data is formatted as comma-separated values.
    This is expected when piping output from `id3c geocode` directly into this
    command.

    Lookup results are output to stdout as comma-separated values, with
    location identifier as <scale>_identifier.
    """
    input_df = load_input_from_file_or_stdin(filename)
    lat_lngs = extract_lat_lng_from_input(input_df, lat_column, lng_column)

    db = DatabaseSession()
    locations = []

    for lat_lng in lat_lngs:
        location = location_lookup(db, lat_lng, scale)
        locations.append(location.identifier if location else None)

    output_df = input_df.copy()
    output_df[f"{scale}_identifier"] = locations

    if drop_latlng_columns:
        try:
            output_df.drop(columns = [lat_column, lng_column], inplace = True)
        except KeyError as error:
            LOG.error(f"{error}. Columns are: {list(output_df.columns)}")
            raise error from None

    output_df.to_csv(sys.stdout, index = False)


def extract_lat_lng_from_input(lookup_input: pd.DataFrame,
                               lat_column: str,
                               lng_column: str) -> pd.Series:
    """
    Extract lat/lng from *lookup_input* using the provided *lat_column* and
    *lng_column* to return as a pandas Series of tuples.

    Raises `KeyError` if either column name cannot be found in *lookup_input*.
    """
    try:
        lat = lookup_input[lat_column]
        lng = lookup_input[lng_column]

    except KeyError as key:
        LOG.error(f"Column «{key}» not found in input columns {list(lookup_input.columns)}")
        raise key from None

    return pd.Series(list(zip(lat, lng)))


def location_lookup(db: DatabaseSession,
                    lat_lng: Tuple[float, float],
                    scale: str) -> Optional[MinimalLocationRecord]:
    """
    Find location within warehouse of *scale* that contains
    *lat_lng* and return location id and identifier.

    Returns None if *lat_lng* contains NaN and if location could not be
    found within ID3C.
    """
    lat, lng = lat_lng

    if not lat or not lng:
        LOG.error("Cannot find location without lat/lng")
        return None

    data = {
        "scale": scale,
        "lat": lat,
        "lng": lng
    }

    # SRID 4326 = EPSG 4326 = World Geodetic System 1984 (WGS84)
    location = db.fetch_row("""
        select location_id as id, identifier, scale
          from warehouse.location
         where scale = %(scale)s and
               st_contains(polygon, st_setsrid(st_point(%(lng)s, %(lat)s), 4326))
        order by identifier asc
        limit 1
        """, data)

    if not location:
        LOG.error(f"No location of scale «{scale}» found for lat/lng")
        return None

    LOG.debug(f"Found location {location.id} «{location.identifier}»")
    return location
