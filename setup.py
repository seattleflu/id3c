from setuptools import setup, find_packages
from pathlib    import Path

base_dir     = Path(__file__).parent.resolve()
version_file = base_dir / "lib/id3c/__version__.py"
readme_file  = base_dir / "README.md"

# Eval the version file to get __version__; avoids importing our own package
with version_file.open() as f:
    exec(f.read())

# Get the long description from the README file
with readme_file.open(encoding = "utf-8") as f:
    long_description = f.read()

setup(
    name = "id3c",
    version = __version__,

    packages = find_packages("lib"),
    package_dir = {"": "lib"},
    package_data = {"": ["data/*"]},

    description = "Infectious Disease Data Distribution Center",
    long_description = long_description,
    long_description_content_type = "text/markdown",

    url = "https://github.com/seattleflu/id3c",
    project_urls = {
        "Bug Reports": "https://github.com/seattleflu/id3c/issues",
        "Source":      "https://github.com/seattleflu/id3c",
    },

    classifiers = [
        "Development Status :: 5 - Production/Stable",

        # This is a CLI
        "Environment :: Console",
        "Environment :: Web Environment",

        # This is for bioinformatic software devs and researchers
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",

        # Prod uses Python 3.9
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
    ],

    # Install an id3c program which calls id3c.cli.cli()
    #   https://setuptools.readthedocs.io/en/latest/setuptools.html#automatic-script-creation
    entry_points = {
        "console_scripts": [
            "id3c = id3c.cli:cli",
        ],
    },

    python_requires = ">=3.8",

    install_requires = [
        "cachetools",
        "click >=8.0, <8.1.4",
        "colorama",
        "deepdiff",
        "fhir.resources <6.0",
        "fiona",
        "flask",
        "fsspec",
        "google-api-python-client",
        "jsonschema",
        "more-itertools",
        "oauth2client >2.0.0,<4.0.0",
        "openpyxl ==3.0.10",
        "pandas ==1.5.3",
        "psycopg2-binary >=2.8,<3",
        "pyyaml",
        "requests",
        "s3fs ==0.4.2",
        "smartystreets-python-sdk ==4.11.12",
        "types-python-dateutil",
        "typing_extensions >=3.7.4",
        "xlrd",

        # We use pkg_resources, which (confusingly) is provided by setuptools.
        # setuptools is nearly ever-present, but it can be missing!
        "setuptools",
    ],

    extras_require = {
        "dev": [
            "mypy",
            "pylint",
            "pytest >=6.2.5,!=7.0.0",
            "sqlparse",
            "types-pkg_resources",
            "types-PyYAML",
            "types-requests",
        ],
    },
)
