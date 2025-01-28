#!/bin/sh

SDSSCORE_APO_TOO=$SDSSCORE_DIR/apo/too_targets
SDSSCORE_LCO_TOO=$SDSSCORE_DIR/lco/too_targets
SDSS_ID_DIFFS_PATH=/uufs/chpc.utah.edu/common/home/sdss50/sdsswork/sandbox/sdss_id/too_diffs

TOO_TARGET_FILES=/uufs/chpc.utah.edu/common/home/sdss50/sdsswork/mos/too/targets

TOO_DBUSER=sdss
TOO_DBHOST=operations.sdss.org
TOO_DBNAME=sdss5db

DATE=`date "+%Y-%m-%d"`

cd /uufs/chpc.utah.edu/common/home/sdssunit/software/github/sdss/too
source .venv/bin/activate

module load sdsscore/main

too process --ignore-invalid --cross-match --run-carton -v $TOO_TARGET_FILES

too dump-targets "$SDSSCORE_APO_TOO"/too_targets_apo_${DATE}.parquet APO

mkdir -p $SDSS_ID_DIFFS_PATH/$DATE
too dump-sdss-id --root $SDSS_ID_DIFFS_PATH/$DATE $DATE

SDSS_ID_FLAT_DIFFS=$SDSS_ID_DIFFS_PATH/$DATE/sdss_id_flat.csv
SDSS_ID_STACKED_DIFFS=$SDSS_ID_DIFFS_PATH/$DATE/sdss_id_stacked.csv

psql -h pipelines.sdss.org -U sdss -d sdss5db -c "\copy catalogdb.sdss_id_flat FROM '$SDSS_ID_FLAT_DIFFS' CSV HEADER;"
psql -h pipelines.sdss.org -U sdss -d sdss5db -c "\copy catalogdb.sdss_id_stacked FROM '$SDSS_ID_STACKED_DIFFS' CSV HEADER;"
