## About The Project

Python extract and transform pipeline of UK PV generation data extracted from the PV_Live API provided by the University of Sheffield Sheffield Solar project.

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

TBC

### Prerequisites

TBC

## Usage

TBC

## Roadmap

- [x] Extract PES region list
- [x] Transform and store PES region list to database
    - [x] Store PES region list to MongoDB
    - [x] Store PES region list to PostgresSQL
- [x] Extract datetime bounded (June '25) regions PV generation
- [x] Transform datetime bounded (June '25) regions PV generation
    - [x] Store PES regions list to MongoDB
    - [ ] Store PES regions list to PostgresSQL

## Contact

James Chadwick - james@j-chadwick.co.uk

Project Link: [dataeng-pipeline](https://github.com/jachad83/dataeng-pipeline)

## Acknowledgments

* [Sheffield University Solar project](https://www.solar.sheffield.ac.uk/api/)
