# Containers

Docker Compose allows us to orchestrate a network of Docker containers to work together through a `docker-compose.yml` file. You can think of the system like a group of computers on a LAN together.

### System Overview

Sunbeam, when running through Docker Compose, consists of five Docker containers,

1. A Prefect Server instance, which orchestrates and manages the different deployments of our data pipeline, schedule flows, as well as hosting the Prefect UI as well as manages deployments and schedules flows 
2. A PostreSQL instance, used by Prefect for its our internal database
3. A Prefect Agent, which actually runs flows (more on this below, understanding this is critical)
4. A MongoDB instance, which contains all of Sunbeam’s own data.
5. An instance of the external API application, which handles client requests and queries the database, as well as communicating with Prefect for the user.

To anyone on the UBC Solar Tailnet, the Prefect dashboard, hosted by the server, is available at [dashboard.sunbeam.ubcsolar.com](http://dashboard.sunbeam.ubcsolar.com), and the API is available at [api.sunbeam.ubcsolar.com](http://api.sunbeam.ubcsolar.com). 

### Agent

There's a bit of nuance to the agent. What code is actually inside? Critically, the agent will contain the Python environment with dependencies as they were when the Docker container was built, but not the code as it was at the time. When it needs to run a flow, it pulls the GitHub repository for the branch/tag that it is supposed to run, and runs the pipeline.   

That is, there is no need to rebuild the agent when pipeline changes, but it does need to be rebuilt when dependencies change. Alternatively, one can manually install new dependencies in the running container. 

Notably, if the infrastructure associated with running the pipeline changes, the containers may need to be rebuilt. If the pipeline **itself** changes, containers do **not** need to be rebuilt.

> Future developers of Sunbeam may want to add some logic to the agent that has it load the dependencies from the pipeline it pulls from GitHub, but that is not something that has been implemented yet! Right now, either the agent can be rebuilt, or you can directly `docker exec -it` into the agent container and install new dependencies. 

