# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

from importlib.machinery import SourceFileLoader
from setuptools import find_packages, setup

import sys


def get_version():
    return SourceFileLoader("version", "aiven_db_migrate/migrate/version.py").load_module().__version__


setup(
    author="Aiven",
    author_email="support@aiven.io",
    entry_points={
        "console_scripts": [
            "pg_migrate = aiven_db_migrate.migrate.pgmigrate:main",
        ],
    },
    install_requires=[
        "psycopg2",
        "packaging",
    ],
    python_requires=">=3.10",
    license="Apache 2.0",
    name="aiven-db-migrate",
    packages=find_packages(exclude=["test"]),
    platforms=["POSIX", "MacOS", "Windows"],
    description="Aiven database migration tool",
    long_description=open("README.md").read(),
    url="https://aiven.io/",
    version=get_version(),
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
