# weatheh.com project

Project used to build weatheh.com. Provides Canadians with simple location based weather data. All weather data is provided by _Environment and Climate Change Canada_.
The aim is to have a simple as possible interface with just the data.
Uses the user's geo location if available or a search for common Canadian cities and locations.

## Getting Started

Build the database with `utils.build_new_database`. This is how the site builds its own.
It gathers data from _Environment and Climate Change Canada_, geonames.org to complement it.
Only _Environment and Climate Change Canada_ is used as an external API when serving requests.


<!--
### Prerequisites

Python 3.6 and up.
Currently using Requests, Sqlalchemy, Pytz and Flask

### Installing

A step by step series of examples that tell you how to get a development env running

Say what the step will be

```
Give the example
```

And repeat

```
until finished
```

End with an example of getting some data out of the system or using it for a little demo

## Running the tests

Explain how to run the automated tests for this system

### Break down into end to end tests

Explain what these tests test and why

```
Give an example
```

### And coding style tests

Explain what these tests test and why

```
Give an example
```

## Deployment

Add additional notes about how to deploy this on a live system

-->
## Built With

* [Request](http://docs.python-requests.org/) - Fetching data
* [SQLAlchemy](https://www.sqlalchemy.org/) - For ORM
* [pytz](http://pytz.sourceforge.net/) - Used to generate RSS Feeds
* [Flask](http://flask.pocoo.org) For now, not sure yet.

* [Weather data from Environment and Climate Change Canada](http://dd.weather.gc.ca/about_dd_apropos.txt) For weather data
* [GeoNames](https://www.geonames.org) For additional city data. Used when building the database only.

<!--
## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags). 

-->

## Authors

* **Maxime Lapointe** 


## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc
