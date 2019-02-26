# Seattle Flu Study database

[PostgreSQL 10](https://www.postgresql.org/about/news/1786/)
on [AWS](https://aws.amazon.com/rds/postgresql/), soon to be
[Azure](https://azure.microsoft.com/en-us/services/postgresql/).

Eventually upgrading to [PostgreSQL 11](https://www.postgresql.org/about/news/1894/)
when it becomes available on our cloud provider.

Initially aims to provide:

* Access via SQL and REST APIs
  ([seattleflu/api](https://github.com/seattleflu/api)
  and maybe [PostgREST](http://postgrest.org) later)
  initially (Python 3.7 and ES2018 web app to come later)

* De-identified metadata for participants (age, sex, address token, etc.) and
  samples (tissue, date, location, etc.)

* Sample diagnostic results (positive/negative for influenza, RSV, and more)

* Sequencing read sets and genome assemblies stored in the cloud and referenced
  via URLs in database

* Rich data types (key/value, JSON, geospatial, etc)

* Strong data integrity and validation controls

* Role-based authentication and restricted data fields using row and
  column-level access control

* Encrypted at rest and TLS-only connections

* Administration via standard command-line tools (and maybe later
  [pgAdmin4](https://www.pgadmin.org/))


## Guidelines

General principles to follow when developing the schema.

* Columns should be maximally typed and constrained, unless there exists a
  concrete use case for something less.

* Consider if a column should ever be unknown (null).

* Consider if a column should have a default.

* Consider what constraints make sense at both a column- and table-level.
  Would a `CHECK` constraint be useful to express domain logic?

* Write a description (comment) for all schemas, tables, columns, etc.

* Grant only the minimal privileges necessary to the read-only and read-write
  roles.  For example, if the read-write role isn't expected to `UPDATE`
  existing records, then only grant it `INSERT`.

* Consider expected data access patterns and create indexes to match.


## Integration with other data systems

Although we're building our own data system, we want to design and create it
with interoperability in mind.  To this extent, our system should adopt or
parallel practices and terminology from other systems when appropriate.
For example:

* Nouns (tables, columns, etc) in our system should consider adopting the
  equivalent terms used by [FHIR R4](http://www.hl7.org/implement/standards/fhir/)
  resources.
  
  This will aid with producing FHIR documents in the future and provides a
  consistent terminology on which to discuss concepts more broadly than our
  group.  FHIR is a large specification and there is a lot to digest; it's
  easy to be daunted or confused by it, so please don't hesitate to ask
  questions.

* Value vocabulary (specimen types, organism names, diagnostic tests, etc)
  should consider using and/or referencing the preferred terms from an
  appropriate ontology like
  [SNOMED CT](https://www.snomed.org/snomed-ct/why-snomed-ct),
  [LOINC](https://loinc.org),
  or [GenEpiO](https://genepio.org/).


## Deploying

The database schema is deployed using [Sqitch](https://sqitch.org), a database
change management tool that really shines.  You can install it a large number
of ways, so pick the one that makes most sense to you.

You'll also need a PostgreSQL server and superuser credentials for it.  The
following commands assume the database server is running locally and your local
user account maps directly to a database superuser.

Create the database with a name of your choosing using the standard Pg tools.
In this case, I've chosen `testing` as the name.

    createdb --encoding=UTF-8 testing

Then use `sqitch` to deploy to it.

    sqitch deploy db:pg:testing

Now you can connect to it for interactive use with:

    psql testing
