
# MongoDB to Postgres

* Dump the current MongoDB and create a local copy (only the measurement collection is required, best to exclude any indices during import and then create only indices on time and geometry).
* Use `exporter/index.js` to export the MongoDB to Postgres (requires a existing database, see the script for configuration options). Install required dependencies with `npm install`.
* Apply `exporter/sql/postprocess.sql` to the resulting Postgres database.

# ArcPy

## Axis Matching
* Create a FileGDB with the feature classes from the Postgres database. This can be done using `arcpy/postgres_to_fgdb.py` (untested, did it manually). This is done because using Postgres directly fails most of the time...
* Adjust paths in `arcpy/config.py` as necessary.
* Run `arcpy/preprocess_axes.py`.
* Run `arcpy/create_subsets.py` to create a subset of measurements with matching axis segments. This will create a new FileGDB and will take about 6.5 hours... The also will be a number of CSV files in the workspace directory.

## Analysis
* Use `arcpy/split_fgdb.py` to create one FileGDB per week.
* Run `arcpy/calculate_statistics.py` to create the actual analysis (takes about half an hour).
* Use `arcpy/to_cest.py` to create a copy of the outputs with CEST instead of UTC times.


Already created and necessary files:

* `\\FILE-SERVER\projects\TSC MGladbach 2015\07_Data\model.zip` (The old axis model)
* `\\FILE-SERVER\projects\TSC MGladbach 2015\07_Data\model_neu.zip` (The current axis model)
* `\\FILE-SERVER\projects\TSC MGladbach 2015\07_Data\envirocar_new.gdb.zip` (FileGDB with the contents of the database for the first 4 weeks)
* `\\FILE-SERVER\projects\TSC MGladbach 2015\07_Data\csv_exports.zip` (The CSV exports created during `create_subsets.py`)
* `\\FILE-SERVER\projects\TSC MGladbach 2015\07_Data\outputs.zip` (The result FileGDB)
* `\\FILE-SERVER\projects\TSC MGladbach 2015\07_Data\outputs_cest.zip` (The result FileGDB in CEST)
* `\\FILE-SERVER\projects\TSC MGladbach 2015\07_Data\outputs_by_week.zip` (The result FileGDB for each single week)
* `\\FILE-SERVER\projects\TSC MGladbach 2015\07_Data\outputs_by_week_cest.zip` (The result FileGDB for each single week in CEST)