CREATE SCHEMA catalogdb;
CREATE SCHEMA targetdb;

CREATE TABLE catalogdb.catalog (
    catalogid BIGINT,
    iauname TEXT,
    ra DOUBLE PRECISION,
    dec DOUBLE PRECISION,
    pmra REAL,
    pmdec REAL,
    parallax REAL,
    lead TEXT,
    version_id INTEGER
);

CREATE TABLE catalogdb.version (
    id SERIAL PRIMARY KEY,
    plan TEXT NOT NULL UNIQUE,
    tag TEXT NOT NULL
);

CREATE TABLE catalogdb.sdss_id_stacked (
    catalogid21 BIGINT,
    catalogid25 BIGINT,
    catalogid31 BIGINT,
    ra_sdss_id DOUBLE PRECISION,
    dec_sdss_id DOUBLE PRECISION,
    sdss_id BIGINT
);

CREATE TABLE catalogdb.gaia_dr3_source (
    source_id BIGINT,
    ra DOUBLE PRECISION,
    dec DOUBLE PRECISION,
    pmra DOUBLE PRECISION,
    pmdec DOUBLE PRECISION,
    parallax DOUBLE PRECISION,
    phot_g_mean_flux DOUBLE PRECISION,
    phot_g_mean_mag REAL,
    phot_bp_mean_flux DOUBLE PRECISION,
    phot_bp_mean_mag REAL,
    phot_rp_mean_flux DOUBLE PRECISION,
    phot_rp_mean_mag REAL,
    l DOUBLE PRECISION,
    b DOUBLE PRECISION
);

CREATE TABLE catalogdb.twomass_psc (
    pts_key INTEGER,
    designation TEXT,
    ra DOUBLE PRECISION,
    decl DOUBLE PRECISION,
    j_m REAL,
    h_m REAL,
    k_m REAL,
    glat DOUBLE PRECISION,
    glon DOUBLE PRECISION
);

CREATE TABLE catalogdb.sdss_dr13_photoobj (
    objid BIGINT,
    ra DOUBLE PRECISION,
    dec DOUBLE PRECISION,
    psfmag_u REAL,
    psfmag_g REAL,
    psfmag_r REAL,
    psfmag_i REAL,
    psfmag_z REAL,
    fibermag_u REAL,
    fibermag_g REAL,
    fibermag_r REAL,
    fibermag_i REAL,
    fibermag_z REAL,
    fiber2mag_u REAL,
    fiber2mag_g REAL,
    fiber2mag_r REAL,
    fiber2mag_i REAL,
    fiber2mag_z REAL,
    l DOUBLE PRECISION,
    b DOUBLE PRECISION,
    run SMALLINT,
    rerun SMALLINT,
    camcol SMALLINT,
    field SMALLINT,
    obj SMALLINT,
    type SMALLINT,
    flags BIGINT
);

CREATE TABLE catalogdb.catalog_to_sdss_dr13_photoobj_primary (
    catalogid BIGINT,
    target_id BIGINT,
    version_id SMALLINT,
    distance REAL,
    best BOOLEAN
);

CREATE TABLE catalogdb.catalog_to_gaia_dr3_source (
    catalogid BIGINT,
    target_id BIGINT,
    version_id SMALLINT,
    distance REAL,
    best BOOLEAN
);

CREATE TABLE catalogdb.catalog_to_twomass_psc (
    catalogid BIGINT,
    target_id BIGINT,
    version_id SMALLINT,
    distance REAL,
    best BOOLEAN
);

CREATE TABLE targetdb.target (
    pk BIGSERIAL PRIMARY KEY,
    ra DOUBLE PRECISION,
    dec DOUBLE PRECISION,
    pmra REAL,
    pmdec REAL,
    epoch REAL,
    parallax REAL,
    catalogid BIGINT
);

CREATE TABLE targetdb.version (
    pk SERIAL PRIMARY KEY,
    plan TEXT UNIQUE,
    tag TEXT,
    target_selection BOOLEAN,
    robostrategy BOOLEAN
);

CREATE TABLE targetdb.instrument (
    pk SERIAL PRIMARY KEY,
    label TEXT UNIQUE,
    default_lambda_eff REAL
);

CREATE TABLE targetdb.carton (
    pk SERIAL PRIMARY KEY,
    mapper_pk INTEGER,
    category_pk INTEGER,
    version_pk INTEGER,
    carton TEXT,
    program TEXT,
    run_on DATE
);

CREATE TABLE targetdb.category (
    pk SERIAL PRIMARY KEY,
    label TEXT
);

CREATE TABLE targetdb.carton_to_target (
    pk BIGSERIAL PRIMARY KEY,
    lambda_eff REAL,
    carton_pk INTEGER,
    target_pk BIGINT,
    cadence_pk INTEGER,
    priority INTEGER,
    value REAL,
    instrument_pk INTEGER,
    delta_ra DOUBLE PRECISION,
    delta_dec DOUBLE PRECISION,
    inertial BOOLEAN,
    can_offset BOOLEAN DEFAULT FALSE
);

CREATE TABLE targetdb.magnitude (
    pk BIGSERIAL PRIMARY KEY,
    g REAL,
    r REAL,
    i REAL,
    h REAL,
    bp REAL,
    rp REAL,
    carton_to_target_pk BIGINT,
    z REAL,
    j REAL,
    k REAL,
    gaia_g REAL,
    optical_prov TEXT
);

\copy catalogdb.catalog FROM PROGRAM 'gzip -dc catalog.csv.gz' WITH CSV HEADER;
\copy catalogdb.sdss_id_stacked FROM PROGRAM 'gzip -dc sdss_id_stacked.csv.gz' WITH CSV HEADER;
\copy catalogdb.catalog_to_gaia_dr3_source FROM PROGRAM 'gzip -dc catalog_to_gaia_dr3_source.csv.gz' WITH CSV HEADER;
\copy catalogdb.catalog_to_sdss_dr13_photoobj_primary FROM PROGRAM 'gzip -dc catalog_to_sdss_dr13_photoobj_primary.csv.gz' WITH CSV HEADER;
\copy catalogdb.catalog_to_twomass_psc FROM PROGRAM 'gzip -dc catalog_to_twomass_psc.csv.gz' WITH CSV HEADER;
\copy catalogdb.gaia_dr3_source FROM PROGRAM 'gzip -dc gaia_dr3_source.csv.gz' WITH CSV HEADER;
\copy catalogdb.sdss_dr13_photoobj FROM PROGRAM 'gzip -dc sdss_dr13_photoobj.csv.gz' WITH CSV HEADER;
\copy catalogdb.twomass_psc FROM PROGRAM 'gzip -dc twomass_psc.csv.gz' WITH CSV HEADER;

ALTER TABLE catalogdb.catalog ADD PRIMARY KEY (catalogid);
ALTER TABLE catalogdb.sdss_id_stacked ADD PRIMARY KEY (sdss_id);
ALTER TABLE catalogdb.catalog_to_gaia_dr3_source ADD PRIMARY KEY (catalogid, target_id, version_id);
ALTER TABLE catalogdb.catalog_to_sdss_dr13_photoobj_primary ADD PRIMARY KEY (catalogid, target_id, version_id);
ALTER TABLE catalogdb.catalog_to_twomass_psc ADD PRIMARY KEY (catalogid, target_id, version_id);
ALTER TABLE catalogdb.sdss_dr13_photoobj ADD PRIMARY KEY (objid);
ALTER TABLE catalogdb.gaia_dr3_source ADD PRIMARY KEY (source_id);
ALTER TABLE catalogdb.twomass_psc ADD PRIMARY KEY (pts_key);

CREATE INDEX ON catalogdb.catalog (version_id);
CREATE INDEX ON catalogdb.catalog (q3c_ang2ipix(ra, dec));

CREATE INDEX ON catalogdb.catalog_to_gaia_dr3_source (catalogid);
CREATE INDEX ON catalogdb.catalog_to_gaia_dr3_source (target_id);
CREATE INDEX ON catalogdb.catalog_to_gaia_dr3_source (best);
CREATE INDEX ON catalogdb.catalog_to_gaia_dr3_source (version_id);

CREATE INDEX ON catalogdb.catalog_to_sdss_dr13_photoobj (catalogid);
CREATE INDEX ON catalogdb.catalog_to_sdss_dr13_photoobj (target_id);
CREATE INDEX ON catalogdb.catalog_to_sdss_dr13_photoobj (best);
CREATE INDEX ON catalogdb.catalog_to_sdss_dr13_photoobj (version_id);

CREATE INDEX ON catalogdb.catalog_to_twomass_psc (catalogid);
CREATE INDEX ON catalogdb.catalog_to_twomass_psc (target_id);
CREATE INDEX ON catalogdb.catalog_to_twomass_psc (best);
CREATE INDEX ON catalogdb.catalog_to_twomass_psc (version_id);

CREATE INDEX ON catalogdb.gaia_dr3_source (q3c_ang2ipix(ra, dec));

CREATE INDEX ON catalogdb.sdss_dr13_photoobj (q3c_ang2ipix(ra, dec));

CREATE INDEX ON catalogdb.twomass_psc (q3c_ang2ipix(ra, decl));

CREATE INDEX ON targetdb.target (catalogid);
CREATE INDEX ON targetdb.carton_to_target (carton_pk);
CREATE INDEX ON targetdb.carton_to_target (target_pk);
CREATE INDEX ON targetdb.magnitude (carton_to_target_pk);
CREATE INDEX ON targetdb.carton (version_pk);

INSERT INTO targetdb.instrument VALUES (0, 'BOSS'), (1, 'APOGEE');
INSERT INTO catalogdb.version VALUES (31, '1.0.0', '1.0.0');

VACUUM ANALYZE catalogdb.catalog;
VACUUM ANALYZE catalogdb.sdss_id_stacked;
VACUUM ANALYZE catalogdb.catalog_to_gaia_dr3_source;
VACUUM ANALYZE catalogdb.catalog_to_sdss_dr13_photoobj_primary;
VACUUM ANALYZE catalogdb.catalog_to_twomass_psc;
VACUUM ANALYZE catalogdb.sdss_dr13_photoobj;
VACUUM ANALYZE catalogdb.gaia_dr3_source;
VACUUM ANALYZE catalogdb.twomass_psc;

VACUUM ANALYZE targetdb.target;
VACUUM ANALYZE targetdb.carton;
VACUUM ANALYZE targetdb.carton_to_target;
VACUUM ANALYZE targetdb.category;
VACUUM ANALYZE targetdb.version;
VACUUM ANALYZE targetdb.magnitude;
