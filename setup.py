from distutils.core import setup

setup(
    name='aiven_db_migrate',
    packages=['aiven_db_migrate'],
    version='1.0.0',
    license='apache-2.0',
    description='Aiven database migration tool. This tool is meant for easy migration of databases from some database '
                'service provider, such AWS RDS, or on premises data center, to Aiven Database as a Service. However, '
                'it\'s not limited for Aiven services and it might be useful as a generic database migration tool.',
    author='Open Source @ Aiven',
    url='https://github.com/aiven/aiven-db-migrate',

    download_url='TBD',
    keywords=['aiven', 'db-migrate'],
    install_requires=[
        'flake8==3.8.4',
        'isort==5.6.4',
        'mypy==0.790',
        'psycopg2==2.8.6',
        'pylint==2.6.0',
        'pytest==6.1.2',
        'yapf==0.30.0',
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache license 2.0',
        'Programming Language :: Python :: 3.0',
        'Programming Language :: Python :: 3.1',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
)
