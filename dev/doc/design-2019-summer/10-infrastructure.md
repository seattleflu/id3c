# Infrastructure

## Azure

We want to move from AWS to Azure.  This should be fine.  It will be a great
point to adopt a configuration system like
[Ansible](https://www.ansible.com/overview/how-ansible-works), and _maybe_ a
cloud provisioning system like
[Terraform](https://learn.hashicorp.com/terraform/getting-started/install.html)
(though I'm 80% sure it won't be worth it).

* Migrate database instances
* Migrate backoffice server and services
* Migrate security groups (firewalls) and private networks

## Containerize?

The "backoffice" is currently:

* Production and testing ID3C API server (Apache + uWSGI)
* Metabase (containerized)
* Lab Labels service (containerized)

For development, would it be useful to produce a "backoffice" container which
bundled?

* ID3C API
* Metabase
* Lab Labels

Another idea is that for both development and deployment, we could instead
produce an "id3c" container which might be used like this:

    # This is the CLI!
    docker run seattleflu/id3c --help
    docker run seattleflu/id3c etl enrollment --commit

    # This is the API, which is either another true CLI command or dispatched
    # by the container entrypoint to uWSGI inside the container.
    docker run seattleflu/id3c web api

    # Testing API would be deployed with a different container image
    docker run seattleflu/id3c:testing web api

    # If we ever had an ID3C frontend
    docker run seattleflu/id3c web frontend

There are some (surmountable) challenges here regarding passing in Pg services
and credentials via the environment and files (with the right permissions), as
well as exporting a uWSGI socket out of the container for the reverse proxy
server.

I'm not clear on the benefits of doing this, other than it would make setting
up a development environment very easy.
