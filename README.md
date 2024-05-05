# MiWaitWay

Track on-time performance and average wait time for MiWay (Mississauga Transit) agency using their public GTFS feed.

Built with Airflow and BigQuery.

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

