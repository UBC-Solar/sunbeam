# Sunbeam

## Requirements

Versions for dependencies (except Python) indicated are recommended

* Git [^1]
* Python >=3.12 [^2]
* Poetry >=1.8.3 [^3]
* Docker >=27.4.0 [^4]
* Docker Compose >=2.31.0 [^5]

## Installation

First, clone the repository from GitHub into a new directory, and enter into the new `sunbeam/` directory.
```bash
git clone https://github.com/UBC-Solar/sunbeam.git
cd sunbeam
```
Then, setup some folders for databases and build Docker containers,
```bash
make build
```
If you ever want to delete the database storage and created docker containers,
```bash
make clean
```

## Usage

### Running with Docker Compose

To run Sunbeam, activate the Docker compose cluster, whilst in the `sunbeam/` folder, run
```bash
docker compose up
```
Add `-d` if you don't want your terminal to be consumed. Feel free to use a window manager like `tmux`.
To stop Sunbeam, whilst in the `sunbeam/` folder, run
```bash
docker compose down
```

### Running Locally

Sunbeam uses [Poetry](https://python-poetry.org/docs/basic-usage/#installing-dependencies) for dependency management. Now, create a new virtual environment.

On MacOS/WSL/Linux,
```bash
python3 -m venv ./venv
source venv/bin/activate
```
then,
```bash
poetry install
```


## Appendix

[^1]: use `git --version` to verify version

[^2]: use `python3 --version` to verify version

[^3]: use `poetry --version` to verify version

[^4]: use `docker --version` to verify version

[^5]: use `docker compose version` to verify version