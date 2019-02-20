# Seattle Flu Study database schemas

[PostgreSQL 11](https://www.postgresql.org/about/news/1894/),
on [AWS](https://aws.amazon.com/rds/postgresql/)
or [Azure](https://azure.microsoft.com/en-us/services/postgresql/).

Initially aims to provide:

* Access via SQL and REST APIs ([PostgREST](http://postgrest.org)) initially
  (Python 3.7 and ES2018 web app to come later)

* Metadata for participants (age, sex, address, etc.) and samples (tissue,
  date, location, etc.)

* Sample diagnostic results (positive/negative for influenza, RSV, and more)

* Sequencing read sets and genome assemblies stored in the cloud and referenced
  via URLs in database

* Rich data types (key/value, JSON, geospatial, etc)

* Strong data integrity and validation controls

* Role-based authentication and restricted data fields using row and
  column-level access control

* Encrypted at rest and TLS-only connections

* Administration via [pgAdmin4](https://www.pgadmin.org/) and standard
  command-line tools


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
