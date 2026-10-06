# AutoElite — BA882 In-Class Project

This repo is the reference implementation for the class. Use it to follow along with labs and as a model for your team project.

> Pull updates as the semester progresses. Do not edit files here — use this as a guide for building your own team repo.

> Consider using this repo as a read-only view, or create your own branch and always pull main, where I update code for you to review for your own team build outs.

---

## Stack

- Cloud Shell / Cloud Editor
- Airflow managed by Astronomer (astro CLI)
- dbt (lives in `include/dbt/` — Astronomer convention)
- GCP: BigQuery, GCS, IAM Service Account, Cloud Run, Vertex AI
- Pinecone (Phase 3)

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/Btibert3/882-202609-class-project.git
cd 882-202609-class-project
```

### 2. Add your service account key

Place your GCP service account JSON key at the **project root** and name it `sa-key.json`.
It is gitignored — it will never be committed.

> The lab pages and resources discuss this pattern directly.

```bash
# confirm it's there
ls sa-key.json
```

### 3. Configure your environment variables

Two things need to live in your `~/.bashrc` so they are available to dbt and other tools running outside of Airflow:

The lab pages discuss how to set an environment variable.  You do not need to write bash commands.  In Cloud Shell Editor at the root of your account, toggle on hidden files, open the file, add the lines __with your valid values__ below, save the file, and you should be ok.

```bash
export GCP_PROJECT=your-gcp-project-id
export GOOGLE_APPLICATION_CREDENTIALS=$HOME/882-202609-class-project/sa-key.json
```
After editing `.bashrc`, reload it via the command below, or as I do, just close/delete the terminal and start with a fresh terminal session.

```bash
source ~/.bashrc
```

### 4. Configure your .env

Copy `.env.example` to `.env` and fill in your values. This is what Airflow reads locally.

```bash
cp .env.example .env
```

The values you need are available on the course site. `GCP_PROJECT` and `GOOGLE_APPLICATION_CREDENTIALS` are already set in your `.bashrc` above — they still need to be in `.env` for Airflow's Docker container to pick them up.

### 5. Load historical flat files into BigQuery

This loads the pre-9/1 historical data into `autoelite_raw`. Run once — it is safe to re-run.

First, make sure your `gcloud` project is set (use the variable you added to `.bashrc`):

```bash
gcloud config set project $GCP_PROJECT
```

Then run the setup SQL:

```bash
bq query --use_legacy_sql=false < setup/load_raw.sql
```

### 6. Start Airflow

```bash
astro dev start
```

Airflow UI at http://localhost:8080 (admin / admin).

### 6b.

The `pipeline_*` dags are what we use to extact and load the data.  I recommend toggling on one at a time.  Once a dag is __active__, because we set the schedule and the parameter `catchup`, Airflow's scheduler will review what hasn't been completed and backfill the data for us.

### 7. Run dbt

```bash
cd include/dbt
dbt seed
dbt run
dbt test
dbt docs generate && dbt docs serve
```

> To stop serving the docs, you can hit control/command + C.

`dbt seed` loads the static reference tables (`reps`, `products`) into BigQuery. Run it before `dbt run` — the staging models for those tables depend on it.

dbt reads `GCP_PROJECT` from your shell environment (set in `.bashrc` above).


---

## Deploying to Astronomer Cloud

### 1. Wake your deployment

In the Astronomer UI, find your deployment and click **Wake**. Wait until the status shows **Running** before proceeding — this can take a few minutes, so be patient.

### 2. Authenticate

Get a token at https://cloud.astronomer.io/token, then:

```bash
astro login -t <your-token>
```

### 3. Deploy

From the project root:

```bash
astro deploy
```

You will be presented with a list of deployments, which may only be a single entry.  Type the number of the entry.

Please be patient.

This deploys the full image — DAGs, `include/`, and `requirements.txt`. The deploy itself takes a few minutes to complete — the CLI will show progress and confirm when it's done.

### 4. Set environment variables in the Astronomer UI

`airflow_settings.yaml` and `.env` are local only — not deployed. Set these in your deployment's **Environment Variables** section in the Astronomer UI:

- `GCP_PROJECT`
- `GCS_BUCKET`
- `AUTOELITE_API_BASE`
- `AUTOELITE_API_KEY`
- `AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT` (your GCP service account connection)

### 5. Set a hibernation schedule

Configure wake/hibernate cron schedules in your deployment settings so the server only runs when your pipelines need it. See the lab page for the exact setup.

---

## For your team project

Your team repo follows this same structure. Key differences:

- Your own GCP project, bucket, and service account
- Your own Astronomer deployment — **do not deploy your team project to the in-class deployment**
- Your own data sources and dbt models built on top of this pattern

The lab pages are the step-by-step guide for all of this.
