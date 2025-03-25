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
tar -cf dp1-dump.tar dp1-dump-test/

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

```