# MiWaitWay

A web app that shows average wait time on a bus stop for MiWay (Mississauga Transit) agency using their public GTFS feed. **Work in progress**.

## Motivation

I like public transport, and I enjoy software engineering.
For a long time, I wanted to build a project that worked with data end-to-end.
From data ingestion throughout the analysis to the presentation to the external end user.
The only thing that stopped me was not finding an analysis topic I would want to dive into (low-effort excuse, I know, but it is what it is).
As I am growing an interest in public transportation and am a day-to-day user of it, I have recently found a topic I would like to explore - an average wait time at a stop.
You can track the project progress by reading ["MiWaitWay" series](https://vmois.dev/tags/#miwaitway) on my engineering blog.

## Deploying Airflow on Google Cloud VM

1. Pull repository
2. Start services with `docker compose up -d`
3. Create SSH tunnel from your local computer to VM instance. In this case, you will not need to expose AIrflow UI to the web.

```bash
gcloud compute ssh airflow-and-web \
    --project miwaitway \
    --zone us-central1-c \
    -- -NL 8080:localhost:8080
```

