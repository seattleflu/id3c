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

        # Python ≥ 3.6 only
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
    ],

    # Install an id3c program which calls id3c.cli.cli()
    #   https://setuptools.readthedocs.io/en/latest/setuptools.html#automatic-script-creation
    entry_points = {
        "console_scripts": [
            "id3c = id3c.cli:cli",
        ],
    },

    python_requires = ">=3.6",

    install_requires = [
        "click >=7.0",
        "colorama",
        "flask",
        "psycopg2 >=2.8,<3",
        "requests",
        "pandas",
        "xlrd",
        "pyyaml",
        "deepdiff",
        "fiona",
    ],
)
