# ID3C: Infectious Disease Data Distribution Center

Data logistics system enabling real-time genomic epidemiology. Built for the
[Seattle Flu Study](https://seattleflu.org).

## Navigation
* [Database](#database)
* [Web API](#web-api)
* [CLI](#cli)
* [Development Setup](#development-setup)

## Database

Currently [PostgreSQL 15](https://www.postgresql.org/about/news/postgresql-15-released-2526/).

Initially aims to provide:

* Access via SQL and [REST APIs](#web-api), with
  [PostgREST](http://postgrest.org) and/or ES2018 web app to maybe come later

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
our web API.

The "warehouse" area contains a hybrid relational + document model utilizing
standard relational tables that each have a JSON column for additional details.
Data enters the warehouse primarily through extract-transform-load (ETL)
routines which process received data and copy it into suitable warehouse rows
and documents.  These ETL routines are run via `bin/id3c etl` subcommands, where
they're defined in Python (though lean heavily on Pg itself).

The "shipping" area contains views of the warehouse designed with specific data
consumers and purposes in mind, such as the incidence modeling team.

While the receiving and shipping areas are expected to be fairly fluid and
reactive to new and changing external requirements, the warehouse area is
expected to change at a somewhat slower pace informed by longer-term vision for
it.

[distribution center]: https://en.wikipedia.org/wiki/Distribution_center
[the log]: https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying


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
  such as `PGHOST` and `PGDATABASE`. You may provide these when starting the
  API server.

  User authentication is performed against the database for each request, so
  you do not (and should not) provide a username and password when starting the
  API server.

* The maximum accepted Content-Length defaults to 20MB.  You can override this
  by setting the environment variable `FLASK_MAX_CONTENT_LENGTH`.

* The `LOG_LEVEL` environment variable controls the level of terminal output.
  Levels are strings: `debug`, `info`, `warning`, `error`.

### Starting the server

The commands `pipenv run python -m id3c.api` or `pipenv run flask run`
will run the application's __development__ server. To provide database
connection details while starting the development server, run the command
`PGDATABASE=DB_NAME pipenv run flask run`, substituting `DB_NAME` with the name
of your database.

For production, a standard `api.wsgi` file is provided which can be used by any
web server with WSGI support.

### Examples

User authentication must be provided when making POST requests to the API. For
example, you can run the following `curl` command to send JSON data named
`enrollments.json` to the `/enrollment` endpoint on a local development server:

```sh
curl http://localhost:5000/enrollment \
  --header "Content-Type: application/json" \
  --data-binary @enrollments.json \
  --user USERNAME
```

Substitute your own local database username for `USERNAME`.  This will prompt
you for a password; you can also specify it directly by using `--user
"USERNAME:PASSWORD"`, though be aware it will be saved to your shell history.

## CLI

Python 3 + [click](https://click.palletsprojects.com)

Interact with the database on the command-line in your shell to:

* Mint identifiers and barcodes

* Run ETL routines, e.g. enrollments, to process received data into the
  warehouse

* Parse, diff, and upload sample manifests.

* Preprocess clinical data and upload it into receiving.

* Send Slack notifications from the [Reportable Conditions Notifications Slack
  App](https://api.slack.com/apps/ALJJAQGKH)

The `id3c` command is the entry point.  It must be run within the project
environment, for example by using `pipenv run id3c`.

The `LOG_LEVEL` environment variable controls the level of terminal output.
Levels are strings: `debug`, `info`, `warning`, `error`.


## Development setup

### Dependencies

Python dependencies are managed using [Pipenv](https://pipenv.readthedocs.io).

Install all the (locked, known-good) dependencies by running:

    pipenv sync

Add new dependencies to `setup.py` and run:

    pipenv lock
    pipenv sync

and then commit the changes to `Pipfile` and `Pipfile.lock`.

### Connection details

Details for connecting to the ID3C database are by convention controlled
entirely by the [standard libpq environment variables](https://www.postgresql.org/docs/current/libpq-envars.html),
[service definitions](https://www.postgresql.org/docs/current/libpq-pgservice.html),
and [password files](https://www.postgresql.org/docs/current/libpq-pgpass.html).

For example, if you want to list the identifier sets available in the Seattle
Flu Study testing database, you could create the following files:

_~/.pg\_service.conf_

    [seattleflu-testing]
    host=testing.db.seattleflu.org
    user=your_username
    dbname=testing

_~/.pgpass_

    testing.db.seattleflu.org:5432:*:your_username:your_password

Make sure the _~/.pgpass_ file is only readable by you since it contains your
password:

    chmod u=rw,og= ~/.pgpass

and then run:

    PGSERVICE=seattleflu-testing pipenv run bin/id3c identifier set ls

These files will also allow you to connect using `psql`:

    psql service=seattleflu-testing

### Tests

Run all tests with:

    pipenv run pytest -v

Run just type-checking tests with:

    ./dev/mypy
