## About The Project

Python module to extract, transform and store UK PV generation data extracted from the PV_Live API provided by the University of Sheffield Sheffield Solar project.

PV_Live API - https://www.solar.sheffield.ac.uk/api/ - https://github.com/SheffieldSolar/PV_Live-API

Load pipeline project: [dataeng-flask](https://github.com/jachad83/dataeng-flask)

### Built With

* [Requests](https://requests.readthedocs.io/en/latest/)
* [pandas](https://pandas.pydata.org/)
* [SQLAlchemy](https://www.sqlalchemy.org/)
* [PyMongo](https://pymongo.readthedocs.io/en/stable/)
* [MongoDB](https://www.mongodb.com/)
* [PostgreSQL](https://www.postgresql.org/)

## Getting Started

Import as a module or run as a script.

### Prerequisites

* requests
* pandas
* sqlalchemy
* pymongo
* datetime
* multiprocessing
* pprint

## Usage

Extract and store data in MongoDB and PostgreSQL DB.

### as import

PvGenerationData(start_date, end_date)
* start_date (optional): Start date of PV generation period data in format YYYY-MM-DD Default: 2025-06-01
* end_date date (optional): Start date of PV generation period data in format YYYY-MM-DD Default: 2025-06-02

### as script

python pvdatacollect.py <start_date> <end_date>
- start_date: Start date of PV generation period data in format YYYY-MM-DD.
- end_date date: Start date of PV generation period data in format YYYY-MM-DD.

## Roadmap

- [ ] Refactor helper functions
- [ ] Unit tests

## Contact

James Chadwick - james@j-chadwick.co.uk

Project Link: [dataeng-pipeline](https://github.com/jachad83/dataeng-pipeline)

## Acknowledgments

* [Sheffield University Solar project](https://www.solar.sheffield.ac.uk/api/)
