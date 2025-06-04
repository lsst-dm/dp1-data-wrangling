# dp1-data-wrangling

Miscellaneous scripts that will be used to prepare Rubin Data Preview 1 data
for transfer to the Rubin Science Platform on Google Cloud.

The current status of the preliminary setup is [on
Confluence](https://rubinobs.atlassian.net/wiki/spaces/DM/pages/492142836/Preliminary+DP1+Butler+at+IDF).

## Sequence of steps to transfer data from USDF TO IDF

At USDF:
```
# Export from Postgres to parquet files.
python export_preliminary_dp1.py
tar -cf dp1-dump.tar dp1-dump/

# Generate a directory full of symlinks pointing
# to the files that will be included in the
# data preview.
python generate_dp1_datastore_symlinks.py

# Transfer artifacts to Google Cloud --
# Switch to a dedicated data transfer node
# with better network bandwidth.
ssh s3dfdtn.slac.stanford.edu
# Start a screen session to keep the transfer going
# if the network drops.
screen
# Copy files to GCS
gcloud auth login
gcloud storage rsync --recursive --no-ignore-symlinks datastore_symlinks gs://butler-us-central1-dp1/
```

Then open up an RSP notebook session in the target IDF environment, and upload the `dp1-dump.tar` file created at USDF.
```
# Load the Butler Registry DB
setup lsst_distrib
tar -xf ~/dp1-dump.tar
python import_preliminary_dp1.py --seed butler-configs/idfdev.yaml # or other seed depending on environment
# Generate an ObsCore table for qserv
butler obscore export --format csv -c ~/repos/dax_obscore/configs/dp1.yaml import-test-repo dp1.csv

# On production, you need to grant the Butler server's database user access to the schema created by
# the importer.
pgcli -h 10.163.1.2 -U your_postgres_user_name dp1
GRANT USAGE ON SCHEMA dm_51058 TO butler
GRANT SELECT ON ALL TABLES IN SCHEMA dm_51058 TO butler
ALTER DEFAULT PRIVILEGES IN SCHEMA dm_51058 GRANT SELECT ON TABLES TO butler
```

### Adding missing dataset types after-the-fact
If you have already done the export/import process to set up the repository, but need to add
additional dataset types, you can follow almost the same process as above with a few tweaks:

```
# Explicitly specify dataset types to export.
python export_preliminary_dp1.py -t some_dataset_type -t other_dataset_type

# Use cp instead of rsync for incremental transfer
cd datastore_symlinks
gcloud storage cp --recursive --no-ignore-symlinks . gs://butler-us-central1-dp1/

# Tell importer that it's OK to add into an existing repository.
python import_preliminary_dp1.py --use-existing-repo
```