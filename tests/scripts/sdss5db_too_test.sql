CREATE SCHEMA catalogdb;
CREATE SCHEMA targetdb;
CREATE SCHEMA sandbox;
CREATE SCHEMA opsdb_apo;
CREATE SCHEMA opsdb_lco;

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
    sdss_id BIGSERIAL PRIMARY KEY,
    last_updated date
);

CREATE TABLE catalogdb.sdss_id_flat (
    sdss_id BIGINT,
    catalogid BIGINT,
    version_id SMALLINT,
    ra_sdss_id DOUBLE PRECISION,
    dec_sdss_id DOUBLE PRECISION,
    n_associated SMALLINT,
    ra_catalogid DOUBLE PRECISION,
    dec_catalogid DOUBLE PRECISION,
    pk BIGSERIAL PRIMARY KEY,
    rank INTEGER
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

-- CREATE TABLE catalogdb.sdss_dr13_photoobj (
--     objid BIGINT,
--     ra DOUBLE PRECISION,
--     dec DOUBLE PRECISION,
--     psfmag_u REAL,
--     psfmag_g REAL,
--     psfmag_r REAL,
--     psfmag_i REAL,
--     psfmag_z REAL,
--     fibermag_u REAL,
--     fibermag_g REAL,
--     fibermag_r REAL,
--     fibermag_i REAL,
--     fibermag_z REAL,
--     fiber2mag_u REAL,
--     fiber2mag_g REAL,
--     fiber2mag_r REAL,
--     fiber2mag_i REAL,
--     fiber2mag_z REAL,
--     l DOUBLE PRECISION,
--     b DOUBLE PRECISION,
--     run SMALLINT,
--     rerun SMALLINT,
--     camcol SMALLINT,
--     field SMALLINT,
--     obj SMALLINT,
--     type SMALLINT,
--     flags BIGINT
-- );

-- CREATE TABLE catalogdb.catalog_to_sdss_dr13_photoobj_primary (
--     catalogid BIGINT,
--     target_id BIGINT,
--     version_id SMALLINT,
--     distance REAL,
--     best BOOLEAN
-- );

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

CREATE TABLE catalogdb.too_target (
    too_id BIGINT PRIMARY KEY,
    program TEXT,
    fiber_type TEXT,
    catalogid BIGINT,
    sdss_id BIGINT,
    gaia_dr3_source_id BIGINT,
    twomass_pts_key INTEGER,
    ra DOUBLE PRECISION,
    dec DOUBLE PRECISION,
    pmra REAL,
    pmdec REAL,
    epoch REAL,
    parallax REAL,
    added_date TIMESTAMP (0) WITH TIME ZONE
);

CREATE TABLE catalogdb.too_metadata (
    too_id BIGINT PRIMARY KEY REFERENCES catalogdb.too_target(too_id),
    sky_brightness_mode TEXT,
    lambda_eff REAL,
    u_mag REAL,
    g_mag REAL,
    r_mag REAL,
    i_mag REAL,
    z_mag REAL,
    optical_prov TEXT,
    gaia_bp_mag REAL,
    gaia_rp_mag REAL,
    gaia_g_mag REAL,
    h_mag REAL,
    delta_ra REAL,
    delta_dec REAL,
    can_offset BOOLEAN,
    inertial BOOLEAN,
    n_exposures SMALLINT,
    priority SMALLINT,
    active BOOLEAN,
    observe_from_mjd INTEGER,
    observe_until_mjd INTEGER,
    observed BOOLEAN,
    last_modified_date TIMESTAMP (0) WITH TIME ZONE
);

CREATE TABLE catalogdb.catalog_to_too_target (
    catalogid BIGINT,
    target_id BIGINT,
    version_id SMALLINT,
    distance REAL,
    best BOOLEAN,
    plan_id TEXT,
    added_by_phase SMALLINT
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

CREATE TABLE targetdb.cadence (
    label TEXT NOT NULL,
    nepochs INTEGER,
    delta DOUBLE PRECISION[],
    skybrightness REAL[],
    delta_max REAL[],
    delta_min REAL[],
    nexp INTEGER[],
    max_length REAL[],
    pk BIGINT PRIMARY KEY,
    obsmode_pk TEXT[],
    label_root TEXT,
    label_version TEXT DEFAULT ''::TEXT
);

CREATE TABLE targetdb.design_mode (
    label TEXT PRIMARY KEY,
    boss_skies_min INTEGER,
    boss_skies_fov DOUBLE PRECISION[],
    apogee_skies_min INTEGER,
    apogee_skies_fov DOUBLE PRECISION[],
    boss_stds_min INTEGER,
    boss_stds_mags_min DOUBLE PRECISION[],
    boss_stds_mags_max DOUBLE PRECISION[],
    boss_stds_fov DOUBLE PRECISION[],
    apogee_stds_min INTEGER,
    apogee_stds_mags_min DOUBLE PRECISION[],
    apogee_stds_mags_max DOUBLE PRECISION[],
    apogee_stds_fov DOUBLE PRECISION[],
    boss_bright_limit_targets_min DOUBLE PRECISION[],
    boss_bright_limit_targets_max DOUBLE PRECISION[],
    boss_trace_diff_targets DOUBLE PRECISION,
    boss_sky_neighbors_targets DOUBLE PRECISION[],
    apogee_bright_limit_targets_min DOUBLE PRECISION[],
    apogee_bright_limit_targets_max DOUBLE PRECISION[],
    apogee_trace_diff_targets DOUBLE PRECISION,
    apogee_sky_neighbors_targets DOUBLE PRECISION[]
);

CREATE TABLE targetdb.field (
    pk SERIAL PRIMARY KEY NOT NULL,
    field_id INTEGER,
    racen DOUBLE PRECISION NOT NULL,
    deccen DOUBLE PRECISION NOT NULL,
    position_angle REAL,
    observatory_pk INTEGER,
    version_pk INTEGER);

CREATE TABLE targetdb.observatory (
    pk SERIAL PRIMARY KEY NOT NULL,
    label TEXT NOT NULL);

CREATE TABLE opsdb_apo.assignment_to_focal (
    pk SERIAL PRIMARY KEY NOT NULL,
    assignment_pk INTEGER,
    configuration_id INTEGER,
    xfocal REAL,
    yfocal REAL,
    positioner_id SMALLINT,
    catalogid BIGINT,
    collided BOOLEAN,
    replaced BOOLEAN);

CREATE TABLE opsdb_lco.assignment_to_focal (
    pk SERIAL PRIMARY KEY NOT NULL,
    assignment_pk INTEGER,
    configuration_id INTEGER,
    xfocal REAL,
    yfocal REAL,
    positioner_id SMALLINT,
    catalogid BIGINT,
    collided BOOLEAN,
    replaced BOOLEAN);

CREATE TABLE opsdb_apo.configuration (
    configuration_id SERIAL PRIMARY KEY NOT NULL,
    design_id INTEGER,
    comment TEXT,
    temperature TEXT,
    epoch DOUBLE PRECISION,
    calibration_version TEXT);

CREATE TABLE sandbox.sdss_id_stacked (
    catalogid21 BIGINT,
    catalogid25 BIGINT,
    catalogid31 BIGINT,
    ra_sdss_id DOUBLE PRECISION,
    dec_sdss_id DOUBLE PRECISION,
    sdss_id BIGSERIAL PRIMARY KEY,
    last_updated date
);

CREATE TABLE sandbox.sdss_id_flat (
    sdss_id BIGINT,
    catalogid BIGINT,
    version_id SMALLINT,
    ra_sdss_id DOUBLE PRECISION,
    dec_sdss_id DOUBLE PRECISION,
    n_associated SMALLINT,
    ra_catalogid DOUBLE PRECISION,
    dec_catalogid DOUBLE PRECISION,
    pk  BIGSERIAL PRIMARY KEY,
    rank INTEGER
);

\copy catalogdb.catalog FROM PROGRAM '/usr/bin/gzip -dc catalog.csv.gz' WITH CSV HEADER;
-- \copy catalogdb.sdss_id_stacked FROM PROGRAM '/usr/bin/gzip -dc sdss_id_stacked.csv.gz' WITH CSV HEADER;
-- \copy catalogdb.sdss_id_flat FROM PROGRAM '/usr/bin/gzip -dc sdss_id_flat.csv.gz' WITH CSV HEADER;
\copy sandbox.sdss_id_stacked FROM PROGRAM '/usr/bin/gzip -dc sdss_id_stacked.csv.gz' WITH CSV HEADER;
\copy sandbox.sdss_id_flat FROM PROGRAM '/usr/bin/gzip -dc sdss_id_flat.csv.gz' WITH CSV HEADER;
\copy catalogdb.catalog_to_gaia_dr3_source FROM PROGRAM '/usr/bin/gzip -dc catalog_to_gaia_dr3_source.csv.gz' WITH CSV HEADER;
-- \copy catalogdb.catalog_to_sdss_dr13_photoobj_primary FROM PROGRAM '/usr/bin/gzip -dc catalog_to_sdss_dr13_photoobj_primary.csv.gz' WITH CSV HEADER;
\copy catalogdb.catalog_to_twomass_psc FROM PROGRAM '/usr/bin/gzip -dc catalog_to_twomass_psc.csv.gz' WITH CSV HEADER;
\copy catalogdb.gaia_dr3_source FROM PROGRAM '/usr/bin/gzip -dc gaia_dr3_source.csv.gz' WITH CSV HEADER;
-- \copy catalogdb.sdss_dr13_photoobj FROM PROGRAM '/usr/bin/gzip -dc sdss_dr13_photoobj.csv.gz' WITH CSV HEADER;
\copy catalogdb.twomass_psc FROM PROGRAM '/usr/bin/gzip -dc twomass_psc.csv.gz' WITH CSV HEADER;
\copy targetdb.version FROM PROGRAM '/usr/bin/gzip -dc targetdb_version.csv.gz' WITH CSV HEADER;
\copy targetdb.field FROM PROGRAM '/usr/bin/gzip -dc targetdb_field.csv.gz' WITH CSV HEADER;
\copy targetdb.design_mode FROM 'design_mode.csv' WITH CSV HEADER;

INSERT INTO catalogdb.version VALUES (31, '1.0.0', '1.0.0');
INSERT INTO targetdb.cadence VALUES ('bright_1x1', 1, '{0}', '{1}', '{0}', '{0}', '{1}', '{0}', 1, null, 'bright_1x1');
INSERT INTO targetdb.instrument VALUES (0, 'BOSS'), (1, 'APOGEE');
INSERT INTO targetdb.observatory VALUES (0, 'APO'), (1, 'LCO');

ALTER TABLE catalogdb.catalog ADD PRIMARY KEY (catalogid);
ALTER TABLE catalogdb.catalog_to_gaia_dr3_source ADD PRIMARY KEY (catalogid, target_id, version_id);
-- ALTER TABLE catalogdb.catalog_to_sdss_dr13_photoobj_primary ADD PRIMARY KEY (catalogid, target_id, version_id);
ALTER TABLE catalogdb.catalog_to_twomass_psc ADD PRIMARY KEY (catalogid, target_id, version_id);
-- ALTER TABLE catalogdb.sdss_dr13_photoobj ADD PRIMARY KEY (objid);
ALTER TABLE catalogdb.gaia_dr3_source ADD PRIMARY KEY (source_id);
ALTER TABLE catalogdb.twomass_psc ADD PRIMARY KEY (pts_key);

-- ALTER TABLE ONLY catalogdb.too_target
--     ADD CONSTRAINT gaia_dr3_source_id_fk
--     FOREIGN KEY (gaia_dr3_source_id) REFERENCES catalogdb.gaia_dr3_source(source_id);

-- ALTER TABLE ONLY catalogdb.too_target
--     ADD CONSTRAINT twomass_pts_key_fk
--     FOREIGN KEY (twomass_pts_key) REFERENCES catalogdb.twomass_psc(pts_key);

ALTER TABLE ONLY targetdb.field
    ADD CONSTRAINT observatory_fk
    FOREIGN KEY (observatory_pk) REFERENCES targetdb.observatory(pk);
ALTER TABLE ONLY targetdb.field
    ADD CONSTRAINT version_fk
    FOREIGN KEY (version_pk) REFERENCES targetdb.version(pk);

CREATE INDEX ON catalogdb.catalog (version_id);
CREATE INDEX ON catalogdb.catalog (q3c_ang2ipix(ra, dec));

CREATE INDEX ON catalogdb.catalog_to_gaia_dr3_source (catalogid);
CREATE INDEX ON catalogdb.catalog_to_gaia_dr3_source (target_id);
CREATE INDEX ON catalogdb.catalog_to_gaia_dr3_source (best);
CREATE INDEX ON catalogdb.catalog_to_gaia_dr3_source (version_id);

-- CREATE INDEX ON catalogdb.catalog_to_sdss_dr13_photoobj_primary (catalogid);
-- CREATE INDEX ON catalogdb.catalog_to_sdss_dr13_photoobj_primary (target_id);
-- CREATE INDEX ON catalogdb.catalog_to_sdss_dr13_photoobj_primary (best);
-- CREATE INDEX ON catalogdb.catalog_to_sdss_dr13_photoobj_primary (version_id);

CREATE INDEX ON catalogdb.catalog_to_twomass_psc (catalogid);
CREATE INDEX ON catalogdb.catalog_to_twomass_psc (target_id);
CREATE INDEX ON catalogdb.catalog_to_twomass_psc (best);
CREATE INDEX ON catalogdb.catalog_to_twomass_psc (version_id);

CREATE INDEX ON catalogdb.catalog_to_too_target (catalogid);
CREATE INDEX ON catalogdb.catalog_to_too_target (target_id);
CREATE INDEX ON catalogdb.catalog_to_too_target (best);
CREATE INDEX ON catalogdb.catalog_to_too_target (version_id);

CREATE INDEX ON catalogdb.sdss_id_stacked(sdss_id);
CREATE INDEX ON catalogdb.sdss_id_stacked(catalogid21);
CREATE INDEX ON catalogdb.sdss_id_stacked(catalogid25);
CREATE INDEX ON catalogdb.sdss_id_stacked(catalogid31);

CREATE INDEX ON catalogdb.sdss_id_flat(sdss_id);
CREATE INDEX ON catalogdb.sdss_id_flat(catalogid);
CREATE INDEX ON catalogdb.sdss_id_flat(version_id);
CREATE INDEX ON catalogdb.sdss_id_flat(rank);

CREATE INDEX ON sandbox.sdss_id_stacked(sdss_id);
CREATE INDEX ON sandbox.sdss_id_stacked(catalogid21);
CREATE INDEX ON sandbox.sdss_id_stacked(catalogid25);
CREATE INDEX ON sandbox.sdss_id_stacked(catalogid31);

CREATE INDEX ON sandbox.sdss_id_flat(sdss_id);
CREATE INDEX ON sandbox.sdss_id_flat(catalogid);
CREATE INDEX ON sandbox.sdss_id_flat(version_id);
CREATE INDEX ON sandbox.sdss_id_flat(rank);

CREATE INDEX ON catalogdb.gaia_dr3_source (q3c_ang2ipix(ra, dec));

-- CREATE INDEX ON catalogdb.sdss_dr13_photoobj (q3c_ang2ipix(ra, dec));

CREATE INDEX ON catalogdb.twomass_psc (q3c_ang2ipix(ra, decl));

CREATE INDEX ON catalogdb.too_target (catalogid);
CREATE INDEX ON catalogdb.too_target (sdss_id);
CREATE INDEX ON catalogdb.too_target (gaia_dr3_source_id);
CREATE INDEX ON catalogdb.too_target (twomass_pts_key);
CREATE INDEX ON catalogdb.too_target (q3c_ang2ipix(ra, dec));

CREATE INDEX ON targetdb.target (catalogid);
CREATE INDEX ON targetdb.carton_to_target (carton_pk);
CREATE INDEX ON targetdb.carton_to_target (target_pk);
CREATE INDEX ON targetdb.magnitude (carton_to_target_pk);
CREATE INDEX ON targetdb.carton (version_pk);

CREATE UNIQUE INDEX ON targetdb.cadence(pk int8_ops);
CREATE UNIQUE INDEX ON targetdb.cadence(label text_ops);
CREATE UNIQUE INDEX ON targetdb.cadence(label text_ops);
CREATE INDEX ON targetdb.cadence(nepochs int4_ops);

CREATE UNIQUE INDEX ON targetdb.design_mode(label);

CREATE INDEX CONCURRENTLY ON targetdb.field (q3c_ang2ipix(racen, deccen));
CREATE INDEX CONCURRENTLY ON targetdb.field USING BTREE(field_id);
CREATE INDEX CONCURRENTLY ON targetdb.field USING BTREE(observatory_pk);
CREATE INDEX CONCURRENTLY ON targetdb.field USING BTREE(version_pk);

CREATE INDEX CONCURRENTLY ON targetdb.version USING BTREE(plan);

VACUUM ANALYZE catalogdb.catalog;
VACUUM ANALYZE catalogdb.sdss_id_stacked;
VACUUM ANALYZE catalogdb.catalog_to_gaia_dr3_source;
-- VACUUM ANALYZE catalogdb.catalog_to_sdss_dr13_photoobj_primary;
VACUUM ANALYZE catalogdb.catalog_to_twomass_psc;
-- VACUUM ANALYZE catalogdb.sdss_dr13_photoobj;
VACUUM ANALYZE catalogdb.gaia_dr3_source;
VACUUM ANALYZE catalogdb.twomass_psc;

VACUUM ANALYZE targetdb.target;
VACUUM ANALYZE targetdb.carton;
VACUUM ANALYZE targetdb.carton_to_target;
VACUUM ANALYZE targetdb.category;
VACUUM ANALYZE targetdb.version;
VACUUM ANALYZE targetdb.observatory;
VACUUM ANALYZE targetdb.field;
VACUUM ANALYZE targetdb.magnitude;

ALTER SEQUENCE catalogdb.sdss_id_stacked_sdss_id_seq RESTART WITH 300000000;
ALTER SEQUENCE catalogdb.sdss_id_flat_pk_seq RESTART WITH 300000000;
ALTER SEQUENCE sandbox.sdss_id_stacked_sdss_id_seq RESTART WITH 300000000;
ALTER SEQUENCE sandbox.sdss_id_flat_pk_seq RESTART WITH 300000000;

CREATE ROLE sdss_user;

GRANT ALL ON TABLE sandbox.sdss_id_stacked TO sdss_user;
GRANT ALL ON TABLE sandbox.sdss_id_flat TO sdss_user;

ALTER ROLE sdss SET search_path TO public,catalogdb,targetdb,opsdb_apo;
ALTER ROLE sdss_user SET search_path TO public,catalogdb,targetdb,opsdb_apo;
