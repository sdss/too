#!/bin/sh

# Exit on error
set -e

# Load submodules
module use /uufs/chpc.utah.edu/common/home/sdss50/software/modulefiles
module load sdsscore/main
module load fps_calibrations/main

CWD=`pwd`

# Paths where the output files will be written
SDSSCORE_APO_TOO=$SDSSCORE_DIR/apo/too_targets
SDSSCORE_LCO_TOO=$SDSSCORE_DIR/lco/too_targets
SDSS_ID_DIFFS_PATH=/uufs/chpc.utah.edu/common/home/sdss50/sdsswork/sandbox/sdss_id/too_diffs

# Input files, provided by the time domain group
TOO_TARGET_FILES=/uufs/chpc.utah.edu/common/home/sdss50/sdsswork/mos/too/targets

# Robostrategy version, needed by the bright-neighbour code
export RS_VERSION=theta-1

# Database connection parameters
export TOO_DBUSER=sdss
export TOO_DBHOST=operations.sdss.org
export TOO_DBNAME=sdss5db

# Masks for the bright-neighbour code
export BN_HEALPIX=/uufs/chpc.utah.edu/common/home/sdss50/sdsswork/target/bn_healpix

# Current date
DATE=`date "+%Y-%m-%d"`

# Load the virtual environment with the code
cd /uufs/chpc.utah.edu/common/home/sdssunit/software/github/sdss/too/current
source .venv/bin/activate

# Process the ToO targets, update the carton, and update sdss_id tables
too process --ignore-invalid --cross-match --run-carton -v $TOO_TARGET_FILES

# Dump the ToO targets to sdsscore. They will be committed automatically. Create
# a symlink to the current file.
too dump-targets $SDSSCORE_APO_TOO/too_targets_apo_$DATE.parquet --observatory APO
cd $SDSSCORE_APO_TOO
rm -f current
ln -s too_targets_apo_$DATE.parquet current
cd $CWD

# And the same for LCO
too dump-targets $SDSSCORE_LCO_TOO/too_targets_lco_$DATE.parquet --observatory LCO
cd $SDSSCORE_LCO_TOO
rm -f current
ln -s too_targets_lco_$DATE.parquet current
cd $CWD

# Dump the new sdss_id records
mkdir -p $SDSS_ID_DIFFS_PATH/$DATE
too dump-sdss-id --root $SDSS_ID_DIFFS_PATH/$DATE $DATE

SDSS_ID_FLAT_DIFFS=$SDSS_ID_DIFFS_PATH/$DATE/sdss_id_flat.csv
SDSS_ID_STACKED_DIFFS=$SDSS_ID_DIFFS_PATH/$DATE/sdss_id_stacked.csv

# Load the new sdss_id records in the pipelines sdss5db database
module load postgresql/15.3

echo "Loading sdss_id diffs into pipelines database"
psql -h pipelines.sdss.org -U u0931042 -d sdss5db -c "\copy catalogdb.sdss_id_flat FROM '$SDSS_ID_FLAT_DIFFS' CSV HEADER;"
psql -h pipelines.sdss.org -U u0931042 -d sdss5db -c "\copy catalogdb.sdss_id_stacked FROM '$SDSS_ID_STACKED_DIFFS' CSV HEADER;"

# Also update the catalogdb sdss_id tables in operations
echo "Loading sdss_id diffs into operations database"
psql -U sdss -h operations.sdss.org sdss5db -c "\copy catalogdb.sdss_id_flat FROM '$SDSS_ID_FLAT_DIFFS' CSV HEADER"
psql -U sdss -h operations.sdss.org sdss5db -c "\copy catalogdb.sdss_id_stacked FROM '$SDSS_ID_STACKED_DIFFS' CSV HEADER"
