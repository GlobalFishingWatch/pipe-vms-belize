# Pipe VMS Belize

Here will be hosted the code to fetch and normalize the [VMS](https://en.wikipedia.org/wiki/Vessel_monitoring_system) sources from `Belize`.

## How to run?

### Requirements

To run in your local machine you need:
- [docker](https://www.docker.com/)
- [docker-compose](https://docs.docker.com/compose/)
- [Apache-Airflow](https://github.com/apache/airflow)

### Setup

1. Creates a docker volume `gcp` if it is not yet defined.
```bash
$ docker volume create --name=gcp
```
2. Build the docker-compose
```bash
$ docker-compose build
$ docker-compose run gcloud auth login
```
3. Install it to your local version of Apache Airflow
```bash
$ install.sh
```


## Collaboration

The folders have separate meanings when you need to implement or do a change in
this repository.  The folders contains:
* `airflow`: Under the airflow folder you
will find all the python code that will be loaded by the Apache Airflow
scheduler.  There is a script placed at the root of the repo `install.sh` that
install all the content of the `airflow` folder to the `/dags` folder of the
docker container where Apache Airflow is running.
* `pipe_vms_belize`: Under the pipe_vms_belize folder you will find an
 `__init__.py` where the details, version and author are described.
* `scripts`: Under the scripts folder you will find the entrypoint of the
service specified in the `docker-compose.yaml` (usually `run.sh`) and the
bash scripts that the system uses to acomplish the fetch and normalize of the
VMS data from `Belize`.
* `tests`: Under the tests
forlder you will find all the unit tests related to fetch and normalize the VMS
data from `Belize`.

## License

- [Apache License 2004](http://www.apache.org/licenses/LICENSE-2.0.txt)
