# Sunbeam

## Requirements

Sunbeam will require the following dependencies for installation and use,

* Git [^1]
* Docker >=27.4.0 [^4]
* Docker Compose >=2.31.0 [^5]

If you want to run Sunbeam locally (not through Docker), you'll need,
* Python >=3.12 [^2]
* Poetry >=1.8.3 [^3]

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
> **NOTE:** If you are on Linux, you may need to do `sudo make build`.

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
> **NOTE:** If you are on Linux, you may need to do `sudo docker compose up`. If you are on `macOS`, you may need to run `sudo ln -s "$HOME/.docker/run/docker.sock" /var/run/docker.sock` if your Docker socket is not already located at `/var/run/docker.sock`.


Add `-d` if you don't want your terminal to be consumed. Feel free to use a window manager like `tmux`.
To stop Sunbeam, whilst in the `sunbeam/` folder, run
```bash
docker compose down
```

Once everything has launched, navigate to `localhost:4200` to access the Prefect Dashboard. Navigate to Worker Pools -> `docker-work-pool` and find the Edit button on the top right.
Find `Environment Variables` and fill in `SOLCAST_API_KEY`, `INFLUX_ORG`, and `INFLUX_TOKEN`.

### Deploying Pipelines

To deploy a pipeline, navigate to the Sunbeam API at `localhost:8080` and select Pipelines API -> Commission Pipeline and enter in the name of a GitHub branch you want to commission. If you want to build what you have locally, select `Build Local`. 

After deploying a pipeline, it will automatically run a flow. You can view the flow by navigating to the Prefect Dashboard at `localhost:4200`.

You can navigate to Deployments -> `your deployment` -> click Run at the top right to run a flow manually, or create a schedule to automatically run flows.

### Running Locally

Sunbeam uses [uv](https://docs.astral.sh/uv/guides/install-python/) for dependency management. Now, create a new virtual environment.

On MacOS/WSL/Linux,
```bash
python3 -m venv ./venv
source venv/bin/activate
```
then,
```bash
uv sync --locked
```
You should be able to run the entrypoint,
```bash
uv run python3 pipeline/run.py
```

Finally, you'll need to add `INFLUX_TOKEN` and `INFLUX_ORG` in the `.env` file created at `sunbeam/config/.env`, it should look something like,
```env
INFLUX_TOKEN=s4Z9_S6_O09jHy665FtrEdS9_ObY44vR4xMh-wYLSWBkypS0S0ZHQgBvEV2A5LgvQ1IKr8byHes2LA==
INFLUX_ORG=8a0b6Ok98Jh31e96
SOLCAST_API_KEY=s4Z9_S6_O09jHy665FtrEdS9_ObY44vR4xMh
```
> No, those are not real API keys.

## Contributing

As Sunbeam uses branches functionally as deployable data pipelines, it is important to differentiate between branches that are intended to be deployed, and branches that are used for development. As such, branches used for development should be prefixed by `dev-` before continuing in snake case such as `dev-fix_influxdb_ingress`.

## Appendix

[^1]: use `git --version` to verify version

[^2]: use `python3 --version` to verify version

[^3]: use `poetry --version` to verify version

[^4]: use `docker --version` to verify version

[^5]: use `docker compose version` to verify version
