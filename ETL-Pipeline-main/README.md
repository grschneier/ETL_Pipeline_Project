![image](https://github.com/user-attachments/assets/9cd40595-8912-4099-bd3a-e525c8e968a9)

# ETL Pipeline

This program extracts data from APIs and Google Drive, transforms it, and uploads it to a SQL database.

Data References here:
https://docs.google.com/spreadsheets/d/1Qm4x9upXJ7waYL8HJXGSme2UUlO5VirIc0cQx2uvgoM/edit?gid=1063056499#gid=1063056499

## Environment Variables

The pipeline expects several credentials to be available as environment variables or in a `.env`/`keys.env` file:

- `DB_USER` – database username
- `DB_PASSWORD` – database password
- `DB_HOST` – database host

API tokens such as `fb_access_token`, `tiktok_access_token`, `linkedin_access_token`, and the Google Ads credentials shown in `keys.env` should also be provided in the environment.

For accessing Google Drive, set:

- `google_drive_client_secret` – contents of the Drive OAuth client secret JSON
- `google_drive_token` – OAuth token JSON generated for Drive access

## Setup

1. Install Python 3.10+ and clone this repository.
2. Install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   Prefect version 2.13 or newer is required. If you see a
   `PREFECT_SERVER_ALLOW_EPHEMERAL_MODE` validation error, upgrade Prefect:
   ```bash
   pip install -U prefect
   ```
3. Create a `.env` or `keys.env` file and provide all required credentials. A minimal example:
   ```env
   DB_USER=myuser
   DB_PASSWORD=mypassword
   DB_HOST=localhost
   FB_ACCESS_TOKEN=<your fb token>
   TIKTOK_ACCESS_TOKEN=<your tiktok token>
   LINKEDIN_ACCESS_TOKEN=<your linkedin token>
   GOOGLE_ADS_DEVELOPER_TOKEN=<google developer token>
   GOOGLE_ADS_CLIENT_ID=<client id>
   GOOGLE_ADS_CLIENT_SECRET=<client secret>
   GOOGLE_ADS_REFRESH_TOKEN=<refresh token>
   google_drive_client_secret={...json...}
   google_drive_token={...json...}
   ```
   These values can also be exported directly in your shell environment.

## Running the ETL

The Prefect flow is defined in `prefect_pipeline.py`. Start an agent and then trigger the `etl_pipeline` deployment.

Start a local agent:
```bash
prefect agent start --work-queue default
```
or run the agent inside a Docker container:
```bash
docker run --pull always -it --env-file keys.env prefecthq/prefect:2 agent start -q "default"
```
With the agent running, trigger the flow:
```bash
prefect deployment run etl_pipeline
```

For ad-hoc runs you can still execute the script directly:
```bash
python load.py
```
Alternatively build and run the Docker image:
```bash
docker build -t etl-pipeline .
docker run --env-file keys.env etl-pipeline
```
The `load.py` module orchestrates extraction from the various APIs, applies transformations, and loads the final tables into the target MySQL database.

## Workflow Overview

1. **Mapping** – `mapping.generate_mapping()` uses your API credentials to discover advertising accounts.
2. **Extraction** – `extract.py` pulls raw data from Facebook, TikTok, LinkedIn and YouTube APIs.
3. **Transformation** – `transform.py` cleans and standardises the datasets.
4. **Load** – `load.main()` writes the consolidated data into your database and logs the run using `app_logging.py`.
5. **Drive Monitor** – `drive_monitor.py` can ingest Google Sheets for additional reporting.
