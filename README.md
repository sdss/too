# SDSS-V Targets of Opportunity

![Versions](https://img.shields.io/badge/python->=3.12-blue)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Test](https://github.com/sdss/too/actions/workflows/test.yml/badge.svg)](https://github.com/sdss/too/actions/workflows/test.yml)
[![Linting](https://github.com/sdss/too/actions/workflows/lint.yml/badge.svg)](https://github.com/sdss/too/actions/workflows/lint.yml)
[![Docker](https://github.com/sdss/too/actions/workflows/docker.yml/badge.svg)](https://github.com/sdss/too/actions/workflows/docker.yml)
[![codecov](https://codecov.io/gh/sdss/too/graph/badge.svg?token=2ZLPYszyLs)](https://codecov.io/gh/sdss/too)

A package to load and cross-match targets of opportunity (ToO) for SDSS-V.

## Overview

This package deals with all the aspects of loading and processing targets of opportunity. At a high level it:

- Reads one or multiple ToO input files.
- Loads new targets into `catalogdb.too_target` and updates the metadata of existing targets in `catalogdb.too_metadata`.
- Cross-matches the new targets, assigning `catalogids` to new targets and populating the `catalogdb.catalog_to_too_target` table.
- Assigns new `sdss_id` to the new `catalogid` entries. (Potentially. TBD.)
- Runs the `too` carton and updates `targetdb.target`, `targetdb.carton_to_target`, and `targetdb.magnitude` for new ToOs.
- Performs target validation.
- Generates a text file of active ToOs that can be synced to the observatories and is used by [jaeger](https://github.com/sdss/jaeger) to replace targets on the fly when creating new configurations.

## Datamodel

Target of opportunity files must be CSV or [Parquet](https://parquet.apache.org) files with a table/dataframe that conforms to the following structure:

| Column              | Type    | Description                                                                                                          |
|---------------------|---------|----------------------------------------------------------------------------------------------------------------------|
| too_id              | int64   | Unique identifier for the ToO target. [required]                                                                     |
| fiber_type          | string  | Type of fiber to be used to observe the target (`APOGEE` or `BOSS`). [required]                                      |
| catalogid           | int64   | catalogid for this target, if already matched.                                                                       |
| sdss_id             | int64   | sdss_id for this target, if already matched.                                                                         |
| gaia_dr3_source_id  | int64   | Unique identifier of the target in the Gaia DR3 catalog (`source_id` column).                                        |
| twomass_pts_key     | int32   | Unique identifier of the target in the 2MASS catalog (`pts_key` column).                                             |
| sky_brightness_mode | string  | The sky brightness mode. Either `bright` or `dark`.                                                                  |
| ra                  | float64 | The Right Ascension of the target in ICRS coordinates as decimal degrees. [required]                                 |
| dec                 | float64 | The declination of the target in ICRS coordinates as decimal degrees. [required]                                     |
| pmra                | float32 | The proper motion in RA in `mas/yr` as a true angle (the `cos(dec)` factor must have been applied).                  |
| pmdec               | float32 | The proper motion in Dec in `mas/yr` as a true angle.                                                                |
| epoch               | float32 | The epoch of the `ra`/`dec` coordinates. Required when `pmra`/`pmdec` are defined.                                   |
| parallax            | float32 | The parallax in `mas`.                                                                                               |
| lambda_eff          | float32 | The effective wavelength to calculate the atmospheric refraction correction for the target.                          |
| u_mag               | float32 | Sloan `u'` magnitude of the target.                                                                                  |
| g_mag               | float32 | Sloan `g'` magnitude of the target.                                                                                  |
| r_mag               | float32 | Sloan `r'` magnitude of the target.                                                                                  |
| i_mag               | float32 | Sloan `i'` magnitude of the target.                                                                                  |
| z_mag               | float32 | Sloan `z'` magnitude of the target.                                                                                  |
| optical_prov        | string  | Source of the optical magnitudes.                                                                                    |
| gaia_bp_mag         | float32 | Gaia `BP` magnitude of the target.                                                                                   |
| gaia_rp_mag         | float32 | Gaia `RP` magnitude of the target.                                                                                   |
| gaia_g_mag          | float32 | Gaia `G` magnitude of the target.                                                                                    |
| h_mag               | float32 | `H`-band magnitude of the target.                                                                                    |
| delta_ra            | float32 | Fixed RA offset as a true angle, in arcsec.                                                                          |
| delta_dec           | float32 | Fixed Dec offset, in arcsec.                                                                                         |
| can_offset          | boolean | `true` if the ToO is allowed to be offset. Offseting will only occur if target violates magnitude limits. [required] |
| inertial            | boolean | If `true`, the proper motions for this target will be ignored (equivalent to `pmra = pmdec = 0.0`).                  |
| n_exposures         | int16   | The minimum number of exposures required for the ToO to be complete and not assigned anymore. [required]             |
| priority            | int16   | The relative prioriry of this target (10: highest, 1: lowest, 0: the target will be ignored)                         |
| active              | boolean | `true` if the target is active and should be assigned to configurations if possible. [required]                      |
| observe_from_mjd    | int32   | MJD from which the target is considered observable. Default to the current date.                                     |
| observe_until_mjd   | int32   | MJD at which the target should automatically be consider inactive. If empty, the target never expires.               |
| observed            | boolean | `true` if the target has been fully observed and should not be assigned again. [required]                            |

The input file(s) must contain *all* the columns defined above with the correct data formats.

At least one magnitude is required for target validation. If one of the Sloan magnitude is provided, *all* the ugriz values must be provided. A sample, valid ToO file can be found [here](docs/sample.csv).

A file can be validated by using the `validate_too_targets` function, which will also fill nulls in some columns with default values.

The `too_id` must be unique across all targets of opportunity. The ingestion of new ToOs will fail if a ToO is found in the database with the same `too_id`.

MJDs are understood of the MJD on which the night *ends*. For example, a target with `observe_from_mjd=60422` will attempt to observ the target from the night in which MJD 60421 crosses into 60422.

```python
from too import read_too_file, validate_too_targets
df = read_too_file('sample.csv')
validate_too_targets(df)
```

For more details see [this wiki page](https://sdss-wiki.atlassian.net/wiki/x/hhTR). To regenerate this table run

```python
>>> from too.datamodel import datamodel_to_markdown
>>> datamodel_to_markdown()
| Column              | Type    | Description                                                                                            |
|---------------------|---------|--------------------------------------------------------------------------------------------------------|
| too_id              | int64   | Unique identifier for the ToO target [required].                                                       |
| fiber_type          | string  | Type of fiber to be used to observe the target [required].                                             |
...
```
