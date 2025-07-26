# Changelog

## Next version

### 🚀 New

* Add `program` as an optional field to the ToO tables.
* Use the `allow_multiple_bests` option in the cross-match configuration.

### ✨ Improved

* Bump `sdss-target-selection` to `1.4.3`.

### 🔧 Fixed

* Use `>=` when in `dump_targets_to_parquet` when comparing the current MJD with `observe_until_mjd`.
* Handle crash when there are no ellegible targets to dump.


## 1.0.1 - 2025-02-03

### 🐛 Fix

* Modified cronjob script to point to `/uufs/chpc.utah.edu/common/home/sdssunit/software/github/sdss/too/current`.


## 1.0.0 - 2025-02-03

### 🚀 New

* Initial release with ToO ingestion, cross-matching, sdss_id updating, and `jaeger` file generation.
