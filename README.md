# CFL Houses County Scrapers

Scheduled scrapers for Florida county records. Pilot: Lake County code enforcement liens.

## Status

- **Phase 0 (discovery):** Complete. See `../Lake_County_Code_Enforcement_Scraper_Plan.md`.
- **Phase 1 (Lake pilot):** This repo. In progress.

## Architecture

```
county-scrapers/
├── config/lake.yaml                  # Lake-specific config: URLs, doc types, city whitelist
├── scrapers/
│   ├── base/
│   │   ├── oncore_acclaim.py         # Harris OnCore Acclaim clerk client (reusable across FL counties)
│   │   ├── pdf_parser.py             # Cascading identifier extraction from lien PDFs
│   │   └── sheets_writer.py          # Google Sheets service-account upsert
│   └── lake_fl/
│       ├── clerk_lake.py             # Lake clerk config applied to OnCore base
│       ├── pa_lake.py                # Lake Property Appraiser reverse-lookup
│       └── code_enforcement.py       # Orchestrator: pull → parse → enrich → write
├── run.py                            # Local/manual CLI entry point
└── .github/workflows/
    └── lake-code-enforcement.yml     # Scheduled weekly run + manual backfill dispatcher
```

Downstream: Master Sheet → user-owned automation → Podio. The scraper's job ends at writing clean deduped rows into the master Google Sheet.

## One-time setup

### 1. Google Cloud service account (one-time, ~5 minutes)

The scraper needs a service account to write to your Drive. This is free.

1. Go to https://console.cloud.google.com/ and sign in as `CFLHousesLLC@gmail.com`.
2. Create a new project. Name it `cfl-county-scrapers`.
3. In the left menu: **APIs & Services → Library**. Search for and enable:
   - "Google Drive API"
   - "Google Sheets API"
4. Left menu: **IAM & Admin → Service Accounts → Create service account**.
   - Service account name: `cfl-county-scraper`
   - Role: none needed (we grant Drive access via folder sharing instead)
   - Click **Done**.
5. Click the new service account, go to the **Keys** tab, click **Add Key → Create new key → JSON**. A file downloads — this is your credentials file. Keep it safe.
6. Note the service account email address (looks like `cfl-county-scraper@cfl-county-scrapers.iam.gserviceaccount.com`).
7. In Google Drive, open the `County_Data` folder → right-click → **Share**. Paste the service account email. Give it **Editor** access. Uncheck "Notify people". Click **Share**.

### 2. GitHub repo + secret (one-time, ~2 minutes)

1. Create a new **private** repo at https://github.com/new. Suggested name: `cflhouses/county-scrapers`.
2. Upload the contents of this folder to the repo (or push locally via git).
3. In the repo, go to **Settings → Secrets and variables → Actions → New repository secret**:
   - Name: `GOOGLE_CREDENTIALS_JSON`
   - Value: paste the **entire contents** of the service account JSON file from step 1.5
4. Add a second secret for the Drive folder ID:
   - Name: `DRIVE_COUNTY_DATA_FOLDER_ID`
   - Value: the folder ID from the URL when you open `County_Data` in Drive (the long string after `/folders/` in the URL)
5. Add a third secret for the master sheet ID:
   - Name: `LAKE_MASTER_SHEET_ID`
   - Value: the ID from the URL of `Lake_CodeEnforcement_Master` (the long string after `/d/` in the URL)
6. Add a fourth secret for the run log sheet ID:
   - Name: `LAKE_RUN_LOG_SHEET_ID`
   - Value: from the URL of `Lake_CodeEnforcement_RunLog`

### 3. First test run

After setup is complete, trigger the workflow manually to confirm everything is wired up:

1. Go to the **Actions** tab in your GitHub repo.
2. Click the **Lake Code Enforcement** workflow in the left sidebar.
3. Click **Run workflow**. Set the dates to a narrow recent range (e.g., 2026-03-15 to 2026-04-15) for the first test.
4. Watch the run. If it succeeds, check the master sheet — there should be new rows.

If anything errors out, share the log with me and we'll troubleshoot.

## Running locally

For development and debugging, you can run the scraper on your own machine:

```bash
pip install -r requirements.txt
export GOOGLE_CREDENTIALS_JSON="$(cat path/to/service-account.json)"
export LAKE_MASTER_SHEET_ID="<id>"
export LAKE_RUN_LOG_SHEET_ID="<id>"
python run.py lake-code-enforcement --from 2026-03-15 --to 2026-04-15
```

## Schedule

- **Weekly incremental:** Runs every Monday at 6am Eastern, pulling the last 14 days (7-day overlap for safety).
- **Backfill (manual):** Triggered via Actions UI with custom date range. Used for the initial 3-year backfill, done month-by-month.

## Known Phase 1 unknowns

Two items we'll resolve during the first live run:

1. **Document viewer URL pattern** — the Acclaim viewer needs a session-derived `TransactionItemId` rather than a public instrument number. The base client has a `resolve_document_url()` method that we'll fill in once we can observe a real session. Until then, PDF download will fail and the pipeline will continue with metadata-only rows (match tier will read "metadata-only" for those records).
2. **Actual distribution of parcel ID / legal / address / owner fallback tiers** — we designed the cascading match to prefer parcel ID. After the first month's run we'll see actual hit rates and can tune regex patterns if needed.
