# Genomic data store

Integrate genomic data into the database, coupled with external object/blob
storage (Azure Blob Storage, AWS S3, etc).

Encompasses:

* Multiple sets of FASTQ files (raw, human-filtered, etc) for a sample
* Consensus genomes for a (sample, organism/pathogen) tuple

FASTQs should live in a remote object store and be referenced by URL in the
database. Consensus genomes may (?) want to live directly in the database.

## Motivations

1. The assembly pipeline wants to select and download FASTQs from ID3C and
   upload consensus genomes back into ID3C.

2. The augur build wants to select and download consensus genomes with linked
   metadata from ID3C.

3. We want the database to know status of molecular work for progress tracking
   and reporting status back to participant.

## Challenges

1. We will need automated processes for moving data around and an event system
   to trigger fetches.

2. We will need to get NWGC to automate sending of sequence read sets (FASTQs).
   I believe Matthew is working on this already.

3. How does access control in the remote object store interact with access
   control in the database?

4. Does Nextstrain's concept of "strain" (the tuple (host, timepoint, pathogen,
   sequence)) fit into our data model well?
