# Seattle Flu Study informatics API

Python 3 + [Flask](http://flask.pocoo.org)

* Consumes and stores enrollment documents from the Audere backend systems


## Config

* Database connection details are set entirely using the [standard libpq
  environment variables](https://www.postgresql.org/docs/current/libpq-envars.html),
  such as `PGHOST`, `PGUSER`, and `PGDATABASE`.

* The maximum accepted Content-Length defaults to 20MB.  You can override this
  by setting the environment variable `FLASK_MAX_CONTENT_LENGTH`.
