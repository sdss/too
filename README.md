# SDSS-V Targets of Opportunity

![Versions](https://img.shields.io/badge/python->3.12-blue)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Test](https://github.com/sdss/too/actions/workflows/test.yml/badge.svg)](https://github.com/sdss/too/actions/workflows/test.yml)
[![Linting](https://github.com/sdss/too/actions/workflows/lint.yml/badge.svg)](https://github.com/sdss/too/actions/workflows/lint.yml)
[![Docker](https://github.com/sdss/too/actions/workflows/docker.yml/badge.svg)](https://github.com/sdss/too/actions/workflows/docker.yml)
[![codecov](https://codecov.io/gh/sdss/too/graph/badge.svg?token=2ZLPYszyLs)](https://codecov.io/gh/sdss/too)

A package to load and cross-match targets of opportunity (ToO) for SDSS-V.

## Datamodel

Target of opportunity files must be CSV or [Parquet](https://parquet.apache.org) files with a table/dataframe that conforms to the following structure:

| Column              | Type    | Description                                                                                            |
|---------------------|---------|--------------------------------------------------------------------------------------------------------|
| too_id              | int64   | Unique identifier for the ToO target [required].                                                       |
| fiber_type          | string  | Type of fiber to be used to observe the target [required].                                             |
| catalogid           | int64   | catalogid for this target.                                                                             |
| sdss_id             | int64   | sdss_id for this target.                                                                               |
| gaia_dr3_source_id  | int64   | Unique identifier of the target in the Gaia DR3 catalog (`source_id` columns).                         |
| twomass_pts_key     | int32   | Unique identifier of the target in the 2MASS catalog (`pts_key` column).                               |
| sky_brightness_mode | string  | The sky brightness mode. Either `bright` or `dark`.                                                    |
| ra                  | float64 | The Right Ascension of the target in ICRS coordinates as decimal degrees [required].                   |
| dec                 | float64 | The declination of the target in ICRS coordinates as decimal degrees [required].                       |
| pmra                | float32 | The proper motion in RA in mas/yr as a true angle (the cos(dec) factor must have been applied).        |
| pmdec               | float32 | The proper motion in Dec in mas/yr as a true angle.                                                    |
| epoch               | float32 | The epoch of the ra/dec coordinates. Required but only relevant if pmra/pmdec are defined.             |
| parallax            | float32 | The parallax in arcsec.                                                                                |
| lambda_eff          | float32 | The effective wavelength to calculate the atmospheric refraction correction for the target.            |
| u_mag               | float32 | Sloan u' magnitude of the target.                                                                      |
| g_mag               | float32 | Sloan g' magnitude of the target.                                                                      |
| r_mag               | float32 | Sloan r' magnitude of the target.                                                                      |
| i_mag               | float32 | Sloan i' magnitude of the target.                                                                      |
| z_mag               | float32 | Sloan z' magnitude of the target.                                                                      |
| optical_prov        | string  | Source of the optical magnitudes.                                                                      |
| gaia_bp_mag         | float32 |                                                                                                        |
| gaia_rp_mag         | float32 |                                                                                                        |
| gaia_g_mag          | float32 | Gaia G magnitude of the target.                                                                        |
| h_mag               | float32 | H-band magnitude of the target.                                                                        |
| delta_ra            | float32 | Fixed RA offset as a true angle, in arcsec.                                                            |
| delta_dec           | float32 | Fixed Dec offset, in arcsec.                                                                           |
| inertial            | boolean |  if not offset correction should be applied based on target magnitude. 0 otherwise.                    |
| n_exposures         | int16   | The minimum number of exposures required for the ToO to be complete and not assigned anymore.          |
| priority            | int16   |                                                                                                        |
| active              | boolean | 1 if the target is active and should be assigned to configurations if possible.                        |
| expiration_date     | int32   | MJD at which the target should automatically be consider inactive. If empty, the target never expires. |
| observed            | boolean | 1 if the target has been fully observed and should not be assigned again.                              |

For more details see [this wiki page](https://wiki.sdss.org/display/OPS/Implementation+Targets+of+Opportunity). To regenerate this table run

```python
>>> from too.datamodel import datamodel_to_markdown
>>> datamodel_to_markdown()
| Column              | Type    | Description                                                                                            |
|---------------------|---------|--------------------------------------------------------------------------------------------------------|
| too_id              | int64   | Unique identifier for the ToO target [required].                                                       |
| fiber_type          | string  | Type of fiber to be used to observe the target [required].                                             |
...
```
