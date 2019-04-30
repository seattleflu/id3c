# Development
## ID3C Common Tasks

Kairsten's notes. TODO: organize with README.
### Configuring your local development environment
#### Prerequisites
This tutorial assumes you have the following dependencies already installed.
* Git
* Python
* PostgreSQL 

1. Clone the git repo
```sh
git clone git@github.com:seattleflu/id3c.git
```

2. To fill the `seattleflu` database with the backup db copy, run 
`pg_restore --data-only --single-transaction --schema receiving --table enrollment --dbname seattleflu seattleflu-production-<date>.pgdb`

3. To restore the presence/absence data on your local dev instance, you have two options.
   1. **Restore from a flat file backup**, e.g. `pg_restore --dbname seattleflu presence_absence.pgdb`
   2. **Restore from seattleflu-testing**
     First, remove local records via
        ```sh
        psql seattleflu <<<"truncate receiving.presence_absence"
        ```
        Then, run `pg_dump` against the testing database and re-fill your local database with the results.
        
        ```sh
        pg_dump --data-only --table receiving.presence_absence --format custom service=seattleflu-testing | pg_restore --single-transaction --dbname seattleflu
        ```

### Prevent wrapping within psql
Set pager environment variable to `less` and specify which `less` method to use. If there's less than a screen full of information, don't page it.
```sh
PAGER=less LESS=SFRXi psql seattleflu
```
To save these settings, add the following lines to your `~/.psqlrc`.
```sql
\setenv PAGER less
\setenv LESS SRFXi
```

### Psql
Reset the processing log for all rows to be blank. When the enrollments pipeline is run, it will process all rows with blank processing logs for the current revision. These rows will be marked `for update`. 
```sql
update receiving.enrollment set processing_log = '[]';
```

### Running id3c
From within the `id3c` directory, run the following command to test your install.
```sh
PGDATABASE=seattleflu pipenv run ./bin/id3c etl enrollments --help
```

To process `enrollments`, run
```sh
PGDATABASE=seattleflu pipenv run ./bin/id3c etl enrollments --prompt
```

If you don't have `LOG_LEVEL=debug` turned on, your logs should be available on your system via
```sh
grep seattleflu /var/log/syslog
```
