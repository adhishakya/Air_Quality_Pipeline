from setuptools import setup, find_packages

setup(
    name = 'air_quality_pipeline',
    version = '0.1',
    description = 'ETL pipeline for fetching, transforming, and loading air quality data',
    packages = find_packages(include = ['pipeline', 'pipeline.*', 'utils', 'utils.*']),
    install_requires = [
        'pandas',
        'psycopg2',
        'requests',
        'python-dotenv',
    ],
    include_package_data = True,
)