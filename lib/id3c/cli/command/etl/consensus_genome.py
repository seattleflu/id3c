"""
Process consensus genome documents into the relational warehouse.

Consensus genome documents are expected to be JSONs with the following format:

\b
  {
    "sample_identifier": str,
    "reference_organism": str,
    "status": str("complete" or "notMapped"),
    "metadata": { "urls": [str] },
    "summary_stats": {
      "reads": str,
      "align_rate": str,
      "mean_coverage_depths": { "segment[1-8]": float }
    },
    "masked_consensus": [{
      "genomic_sequence": str,
      "sequence_segment": str,
      "sequence_identifier": str
    }]
  }

Note: "summary_stats" and "masked_consensus" are not needed
if "status" is "notMapped"
"""
import click
import logging
from typing import Any, Mapping, NamedTuple, Optional
from textwrap import dedent
from datetime import datetime, timezone
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import Json
from id3c.db.types import GenomeRecord, MinimalSampleRecord, OrganismRecord, SequenceReadSetRecord
from . import etl, find_sample


LOG = logging.getLogger(__name__)


# This revision number is stored in the processing_log of each consensus genome
# record when the consensus genome record is successfully processed by this ETL
# routine. The routine finds new-to-it records to process by looking for
# consensus genome records lacking this revision number in their log.  If a
# change to the ETL routine necessitates re-processing all consensus genome
# records, this revision number should be incremented.
REVISION = 1


@etl.command("consensus-genome", help = __doc__)

@click.option("--dry-run", "action",
    help        = "Only go through the motions of changing the database (default)",
    flag_value  = "rollback",
    default     = True)

@click.option("--prompt", "action",
    help        = "Ask if changes to the database should be saved",
    flag_value  = "prompt")

@click.option("--commit", "action",
    help        = "Save changes to the database",
    flag_value  = "commit")

def etl_consensus_genome(*, action: str):
    LOG.debug(f"Starting the consensus genome ETL routine, revision {REVISION}")

    db = DatabaseSession()

    # Fetch and iterate over consensus genome records that aren't processed
    #
    # Rows we fetch are locked for update so that two instances of this
    # command don't try to process the same consensus genome records.
    LOG.debug("Fetching unprocessed consensus genome records")

    consensus_genome = db.cursor("consensus genome")
    consensus_genome.execute("""
        select consensus_genome_id as id, document, received
          from receiving.consensus_genome
         where not processing_log @> %s
         order by id
           for update
        """, (Json([{ "revision": REVISION }]),))

    processed_without_error = None

    try:
        for record in consensus_genome:
            with db.savepoint(f"consensus genome record {record.id}"):
                LOG.info(f"Processing consensus genome record {record.id}")

                # Verify sample identifier is in the database
                sample = find_sample(db,
                    record.document["sample_identifier"],
                    for_update=False)

                # Most of the time we expect to see existing sequence read sets,
                # but we also want to update the details log. However, the
                # only unique constraint on the sequence_read_set table is
                # defined within a trigger function, as Postgres does not allow
                # unique constraints on array columns. Therefore, perform a
                # find-or-create followed by an update to the details column to
                # avoid conflict.
                sequence_read_set = find_or_create_sequence_read_set(db,
                    record.document, sample)

                status = record.document.get("status")

                # Find the matching organism within the warehouse for the
                # reference organism
                organism_name = get_lineage(db, record.document)
                organism = find_organism(db, organism_name)

                assert organism, f"No organism found with name «{organism_name}»"

                # Only upsert genome and genomic sequences if the assembly job
                # was marked as complete.
                if status == 'complete':
                    # Most of the time we expect to see new sequences, so an
                    # insert-first approach makes the most sense to avoid useless
                    # queries.
                    genome = upsert_genome(db,
                        sequence_read_set = sequence_read_set,
                        organism = organism,
                        document = record.document)

                    for masked_consensus in record.document['masked_consensus']:
                        genomic_sequence = upsert_genomic_sequence(db,
                            genome = genome,
                            masked_consensus = masked_consensus)

                update_sequence_read_set_details(db,
                                                 sequence_read_set.id,
                                                 organism,
                                                 status)
                mark_processed(db, record.id, {"status": "processed"})

                LOG.info(f"Finished processing consensus genome record {record.id}")

    except Exception as error:
        processed_without_error = False

        LOG.error(f"Aborting with error")
        raise error from None

    else:
        processed_without_error = True

    finally:
        if action == "prompt":
            ask_to_commit = \
                "Commit all changes?" if processed_without_error else \
                "Commit successfully processed consensus genome records up to this point?"

            commit = click.confirm(ask_to_commit)
        else:
            commit = action == "commit"

        if commit:
            LOG.info(
                "Committing all changes" if processed_without_error else \
                "Committing successfully processed consensus genome records up to this point")
            db.commit()

        else:
            LOG.info("Rolling back all changes; the database will not be modified")
            db.rollback()


def find_or_create_sequence_read_set(db: DatabaseSession, document: dict, sample: MinimalSampleRecord) -> SequenceReadSetRecord:
    """
    Find sequence read set given a *sample* and consensus genome record
    *document*, inserting the sequence read set if it does not exist.
    Return the sequence read set.
    """
    urls = document['metadata']['urls']
    LOG.debug(dedent(f"""
    Looking up sequence read set with sample ID «{sample.id}» and urls {urls}
    """))

    sequence_read_set: SequenceReadSetRecord = db.fetch_row("""
        select sequence_read_set_id as id, sample_id, urls
          from warehouse.sequence_read_set
         where sample_id = %s
          and urls @> %s
          and %s @> urls
        """, (sample.id, urls, urls))

    if sequence_read_set:
        LOG.info(f"Found sequence read set {sequence_read_set.id}")
    else:
        LOG.debug(dedent(f"""
        Sequence read set not found for sample id «{sample.id}» and urls {urls}
        """))

        data = {
            "sample_id": sample.id,
            "urls": urls,
        }

        sequence_read_set = db.fetch_row("""
            insert into warehouse.sequence_read_set (sample_id, urls)
                values (%(sample_id)s, %(urls)s)
            returning sequence_read_set_id as id, sample_id, urls
            """, data)

        LOG.info(f"Created sequence read set {sequence_read_set.id}")

    return sequence_read_set


def update_sequence_read_set_details(db, sequence_read_set_id: int,
                                     organism: OrganismRecord, status: str) -> None:
    """
    This function is a workaround to the order-specific unique constraint for
    arrays in Postgres. It searches for an existing *sequence_read_set* by its
    ID and updates the details column with the given *entry* details.
    """
    LOG.debug(f"Marking sequence read set data {sequence_read_set_id} as received")

    entry = {
        organism.lineage : { "status": status }
    }

    data = {
        "sequence_read_set_id": sequence_read_set_id,
        "log_entry": Json(entry),
    }

    # Postgres allows array columns to be defined as unique, but the ordering of
    # the arrays must be the same for the arrays to match. We are only
    # interested in matching on array content, not array ordering. This prevents
    # us from being able to use the built-in unique constraint on urls, thereby
    # preventing `ON CONFLICT` updates to a table when urls is the only unique
    # column in the table.
    with db.cursor() as cursor:
        cursor.execute("""
            update warehouse.sequence_read_set
              set details = coalesce(details, '{}') || %(log_entry)s
            where sequence_read_set_id = %(sequence_read_set_id)s
            """, data)


def get_lineage(db: DatabaseSession, document: dict) -> str:
    """
    Given a *document* containing a reference organism, returns its matching
    lineage.
    """
    organism_name = document.get("reference_organism")

    organism_map = {
        'h1n1pdm' : '*.H1N1',
        'h3n2' : '*.H3N2',
        'vic' : '*.Vic',
        'yam' : '*.Yam'
    }

    if organism_name not in organism_map:
        raise UnknownOrganismError(f"Unknown organism name «{organism_name}»")

    return organism_map[organism_name]


def find_organism(db: DatabaseSession, lineage: str) -> Optional[OrganismRecord]:
    """
    Find organism by *lineage* and return organism.
    """
    LOG.debug(f"Looking up organism «{lineage}»")

    organism = db.fetch_row("""
        select organism_id as id, lineage
          from warehouse.organism
         where lineage ~ %s
        """, (lineage,))

    if not organism:
        LOG.error(f"No organism with lineage «{lineage}» found")
        return None

    LOG.info(f"Found organism {organism.id} «{organism.lineage}»")
    return organism


def upsert_genome(db: DatabaseSession, sequence_read_set: SequenceReadSetRecord,
                  organism: OrganismRecord, document: dict) -> GenomeRecord:
    """
    Upsert consensus genomes with the given *sequence_read_set*, *organism*,
    and consensus genome *document*.
    """
    LOG.debug(dedent(f"""
    Upserting genome with sequence read set {sequence_read_set.id},
    organism {organism.id} «{organism.lineage}»"""))

    data = {
        "sample_id": sequence_read_set.sample_id,
        "organism_id": organism.id,
        "sequence_read_set_id": sequence_read_set.id,
        "additional_details": Json(document['summary_stats'])
    }

    genome: GenomeRecord = db.fetch_row("""
        insert into warehouse.consensus_genome (sample_id, organism_id,
            sequence_read_set_id, details)
          values (%(sample_id)s, %(organism_id)s, %(sequence_read_set_id)s,
                %(additional_details)s)

        on conflict (sample_id, organism_id, sequence_read_set_id) do update
            set details = %(additional_details)s

        returning consensus_genome_id as id, sample_id, organism_id, sequence_read_set_id
        """, data)

    assert genome.id, "Upsert affected no rows!"

    LOG.info(dedent(f"""
    Upserted genome {genome.id} with sample ID «{genome.sample_id}»,
    organism ID «{genome.organism_id}», and sequence read set ID «{genome.sequence_read_set_id}»
    """))

    return genome


def upsert_genomic_sequence(db: DatabaseSession, genome: GenomeRecord, masked_consensus: dict) -> Any:
    """
    Upsert genomic sequence given a *genome* record and some information from a
    given *masked_consensus*.
    """
    sequence_identifier = "".join(
        [masked_consensus['sequence_identifier'], "-", str(genome.sequence_read_set_id)])
    LOG.info(f"Upserting genomic sequence «{sequence_identifier}»")

    data = {
        "identifier": sequence_identifier,
        "segment": masked_consensus['sequence_segment'],
        "seq": masked_consensus['genomic_sequence'],
        "genome_id": genome.id,
    }

    genomic_sequence = db.fetch_row("""
        insert into warehouse.genomic_sequence (identifier, segment, seq, consensus_genome_id)
            values (%(identifier)s, %(segment)s, %(seq)s, %(genome_id)s)

        on conflict (identifier) do update
            set seq = excluded.seq,
                segment = excluded.segment

        returning genomic_sequence_id as id, identifier, segment, seq, consensus_genome_id
        """, data)

    assert genomic_sequence.consensus_genome_id == genome.id, \
        "Provided sequence identifier was not unique, matched a sequence linked to another consensus genome!"
    assert genomic_sequence.id, "Upsert affected no rows!"

    LOG.info(f"Upserted genomic sequence {genomic_sequence.id}»")

    return genomic_sequence


def mark_processed(db, consensus_genome_id: int, entry: Mapping) -> None:
    LOG.debug(f"Marking consensus genome document {consensus_genome_id} as processed")

    data = {
        "consensus_genome_id": consensus_genome_id,
        "log_entry": Json({
            **entry,
            "revision": REVISION,
            "timestamp": datetime.now(timezone.utc),
        }),
    }

    with db.cursor() as cursor:
        cursor.execute("""
            update receiving.consensus_genome
               set processing_log = processing_log || %(log_entry)s
             where consensus_genome_id = %(consensus_genome_id)s
            """, data)


class UnknownOrganismError(ValueError):
    """
    Raised by :function:`organism` if a provided organism name is not among the
    set of expected values.
    """
    pass
