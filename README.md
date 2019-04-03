# ID3C: Infectious Disease Data Distribution Center

Data logistics for the [Seattle Flu Study](https://seattleflu.org) and
[Flu@Home](https://fluathome.org), enabling real-time genomic epidemiology
studies.

## Database

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


### Design

The database is designed as a [distribution center][] which receives data from
external providers, repackages and stores it in a data warehouse, and ships
data back out of the warehouse via views, web APIs, and other means.  Each of
these three conceptual areas are organized into their own PostgreSQL schemas
within a single database.

The "receiving" area contains tables to accept minimally- or un-controlled data
from external providers.  The general expectation is that most tables here are
logs ([in the journaling sense][the log]) and will be processed later in
sequential order.  For example, participant enrollment documents from our
consent and questionnaire app partner, Audere, are stored here when received by
[our API][].

The "warehouse" area contains a hybrid relational + document model utilizing
standard relational tables that each have a JSON column for additional details.
Data enters the warehouse primarily through extract-transform-load (ETL)
routines which process received data and copy it into suitable warehouse rows
and documents.  These ETL routines are run via `bin/db etl` subcommands, where
they're defined in Python (though lean heavily on Pg itself).

The "shipping" area contains views of the warehouse designed with specific data
consumers and purposes in mind, such as the incidence modeling team.

While the receiving and shipping areas are expected to be fairly fluid and
reactive to new and changing external requirements, the warehouse area is
expected to change at a somewhat slower pace informed by longer-term vision for
it.

[distribution center]: https://en.wikipedia.org/wiki/Distribution_center
[the log]: https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying
[our API]: https://github.com/seattleflu/api


### Guidelines

General principles to follow when developing the schema.

* Do the ~simplest thing that meets our immediate needs.  Aim for ease of
  modification in the future rather than trying to guess future needs in
  advance.
  
  It can be really hard to stick to this principle, but it turns out that the
  best way to make something flexible for future needs is to make it as simple
  as possible now so it can be modified later.

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


### Integration with other data systems

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


### Deploying

The database schema is deployed using [Sqitch](https://sqitch.org), a database
change management tool that really shines.  You can install it a large number
of ways, so pick the one that makes most sense to you.

### Development

For development, you'll need a PostgreSQL server and superuser credentials for
it.  The following commands assume the database server is running locally and
your local user account maps directly to a database superuser.

Create a database named `seattleflu` using the standard Pg tools.  (You can use
another name if you want, maybe to have different dev instances, but you'll
need to adjust the [sqitch target][] you deploy to.)

    createdb --encoding=UTF-8 seattleflu

Then use `sqitch` to deploy to it.  (`dev` is a [sqitch target][] configured in
_sqitch.conf_ which points to a local database named `seattleflu`.)

    sqitch deploy dev

Now you can connect to it for interactive use with:

    psql seattleflu

### Testing and production

Our [testing and production databases][databases doc] are configured as
`testing` and `production` sqitch targets.  When running sqitch against these
targets, you'll need to provide a username via `PGUSER` and a password via an
entry in _~/.pgpass_.


[sqitch target]: https://metacpan.org/pod/distribution/App-Sqitch/lib/sqitch-target.pod
[databases doc]: https://github.com/seattleflu/documentation/blob/master/infrastructure.md#databases-postgresql


## Web API

Python 3 + [Flask](http://flask.pocoo.org)

* Consumes and stores enrollment documents from the Audere backend systems

### Config

* Database connection details are set entirely using the [standard libpq
  environment variables](https://www.postgresql.org/docs/current/libpq-envars.html),
  such as `PGHOST`, `PGUSER`, and `PGDATABASE`.

* The maximum accepted Content-Length defaults to 20MB.  You can override this
  by setting the environment variable `FLASK_MAX_CONTENT_LENGTH`.

### Starting the server

The commands `pipenv run python -m seattleflu.api` or `pipenv run flask run`
will run the application's __development__ server.

For production, a standard `api.wsgi` file is provided which can be used by any
web server with WSGI support.


## Dependencies

Python dependencies are managed using [Pipenv](https://pipenv.readthedocs.io).

Install all the (locked, known-good) dependencies by running:

    pipenv sync

Add new dependencies to `Pipfile`, run:

    pipenv install <name>

and then commit the changes to `Pipfile` and `Pipfile.lock`.
