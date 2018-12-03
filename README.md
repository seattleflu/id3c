# Seattle Flu Study informatics API

Python 3 + [Flask](http://flask.pocoo.org)

* Consumes and stores enrollment documents from the Audere backend systems


## Running

### Dependencies

Python dependencies are managed using [Pipenv](https://pipenv.readthedocs.io).

Install all the (locked, known-good) dependencies by running:

    pipenv sync

Add new dependencies to `Pipfile`, run:

    pipenv install <name>

and then commit the changes to `Pipfile` and `Pipfile.lock`.


### Config

* Database connection details are set entirely using the [standard libpq
  environment variables](https://www.postgresql.org/docs/current/libpq-envars.html),
  such as `PGHOST`, `PGUSER`, and `PGDATABASE`.

* The maximum accepted Content-Length defaults to 20MB.  You can override this
  by setting the environment variable `FLASK_MAX_CONTENT_LENGTH`.


### Starting the server

The commands `pipenv run python -m seattleflu.api` or `pipenv run flask run`
will run the application's __development__ server.

For production, a standard `wsgi.py` file is provided which can be used by any
web server with WSGI support.
