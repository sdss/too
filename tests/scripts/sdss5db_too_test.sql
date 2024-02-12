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
    solution_id BIGINT,
    designation TEXT,
    source_id BIGINT,
    random_index BIGINT,
    ref_epoch DOUBLE PRECISION,
    ra DOUBLE PRECISION,
    ra_error REAL,
    dec DOUBLE PRECISION,
    dec_error REAL,
    parallax DOUBLE PRECISION,
    parallax_error REAL,
    parallax_over_error REAL,
    pm REAL,
    pmra DOUBLE PRECISION,
    pmra_error REAL,
    pmdec DOUBLE PRECISION,
    pmdec_error REAL,
    ra_dec_corr REAL,
    ra_parallax_corr REAL,
    ra_pmra_corr REAL,
    ra_pmdec_corr REAL,
    dec_parallax_corr REAL,
    dec_pmra_corr REAL,
    dec_pmdec_corr REAL,
    parallax_pmra_corr REAL,
    parallax_pmdec_corr REAL,
    pmra_pmdec_corr REAL,
    astrometric_n_obs_al SMALLINT,
    astrometric_n_obs_ac SMALLINT,
    astrometric_n_good_obs_al SMALLINT,
    astrometric_n_bad_obs_al SMALLINT,
    astrometric_gof_al REAL,
    astrometric_chi2_al REAL,
    astrometric_excess_noise REAL,
    astrometric_excess_noise_sig REAL,
    astrometric_params_solved SMALLINT,
    astrometric_primary_flag BOOLEAN,
    nu_eff_used_in_astrometry REAL,
    pseudocolour REAL,
    pseudocolour_error REAL,
    ra_pseudocolour_corr REAL,
    dec_pseudocolour_corr REAL,
    parallax_pseudocolour_corr REAL,
    pmra_pseudocolour_corr REAL,
    pmdec_pseudocolour_corr REAL,
    astrometric_matched_transits SMALLINT,
    visibility_periods_used SMALLINT,
    astrometric_sigma5d_max REAL,
    matched_transits SMALLINT,
    new_matched_transits SMALLINT,
    matched_transits_removed SMALLINT,
    ipd_gof_harmonic_amplitude REAL,
    ipd_gof_harmonic_phase REAL,
    ipd_frac_multi_peak SMALLINT,
    ipd_frac_odd_win SMALLINT,
    ruwe REAL,
    scan_direction_strength_k1 REAL,
    scan_direction_strength_k2 REAL,
    scan_direction_strength_k3 REAL,
    scan_direction_strength_k4 REAL,
    scan_direction_mean_k1 REAL,
    scan_direction_mean_k2 REAL,
    scan_direction_mean_k3 REAL,
    scan_direction_mean_k4 REAL,
    duplicated_source BOOLEAN,
    phot_g_n_obs SMALLINT,
    phot_g_mean_flux DOUBLE PRECISION,
    phot_g_mean_flux_error REAL,
    phot_g_mean_flux_over_error REAL,
    phot_g_mean_mag REAL,
    phot_bp_n_obs SMALLINT,
    phot_bp_mean_flux DOUBLE PRECISION,
    phot_bp_mean_flux_error REAL,
    phot_bp_mean_flux_over_error REAL,
    phot_bp_mean_mag REAL,
    phot_rp_n_obs SMALLINT,
    phot_rp_mean_flux DOUBLE PRECISION,
    phot_rp_mean_flux_error REAL,
    phot_rp_mean_flux_over_error REAL,
    phot_rp_mean_mag REAL,
    phot_bp_rp_excess_factor REAL,
    phot_bp_n_contaminated_transits SMALLINT,
    phot_bp_n_blended_transits SMALLINT,
    phot_rp_n_contaminated_transits SMALLINT,
    phot_rp_n_blended_transits SMALLINT,
    phot_proc_mode SMALLINT,
    bp_rp REAL,
    bp_g REAL,
    g_rp REAL,
    radial_velocity REAL,
    radial_velocity_error REAL,
    rv_method_used SMALLINT,
    rv_nb_transits SMALLINT,
    rv_nb_deblended_transits SMALLINT,
    rv_visibility_periods_used SMALLINT,
    rv_expected_sig_to_noise REAL,
    rv_renormalised_gof REAL,
    rv_chisq_pvalue REAL,
    rv_time_duration REAL,
    rv_amplitude_robust REAL,
    rv_template_teff REAL,
    rv_template_logg REAL,
    rv_template_fe_h REAL,
    rv_atm_param_origin SMALLINT,
    vbroad REAL,
    vbroad_error REAL,
    vbroad_nb_transits SMALLINT,
    grvs_mag REAL,
    grvs_mag_error REAL,
    grvs_mag_nb_transits SMALLINT,
    rvs_spec_sig_to_noise REAL,
    phot_variable_flag TEXT,
    l DOUBLE PRECISION,
    b DOUBLE PRECISION,
    ecl_lon DOUBLE PRECISION,
    ecl_lat DOUBLE PRECISION,
    in_qso_candidates BOOLEAN,
    in_galaxy_candidates BOOLEAN,
    non_single_star SMALLINT,
    has_xp_continuous BOOLEAN,
    has_xp_sampled BOOLEAN,
    has_rvs BOOLEAN,
    has_epoch_photometry BOOLEAN,
    has_epoch_rv BOOLEAN,
    has_mcmc_gspphot BOOLEAN,
    has_mcmc_msc BOOLEAN,
    in_andromeda_survey BOOLEAN,
    classprob_dsc_combmod_quasar DOUBLE PRECISION,
    classprob_dsc_combmod_galaxy DOUBLE PRECISION,
    classprob_dsc_combmod_star DOUBLE PRECISION,
    teff_gspphot DOUBLE PRECISION,
    teff_gspphot_lower DOUBLE PRECISION,
    teff_gspphot_upper DOUBLE PRECISION,
    logg_gspphot DOUBLE PRECISION,
    logg_gspphot_lower DOUBLE PRECISION,
    logg_gspphot_upper DOUBLE PRECISION,
    mh_gspphot DOUBLE PRECISION,
    mh_gspphot_lower DOUBLE PRECISION,
    mh_gspphot_upper DOUBLE PRECISION,
    distance_gspphot DOUBLE PRECISION,
    distance_gspphot_lower DOUBLE PRECISION,
    distance_gspphot_upper DOUBLE PRECISION,
    azero_gspphot DOUBLE PRECISION,
    azero_gspphot_lower DOUBLE PRECISION,
    azero_gspphot_upper DOUBLE PRECISION,
    ag_gspphot DOUBLE PRECISION,
    ag_gspphot_lower DOUBLE PRECISION,
    ag_gspphot_upper DOUBLE PRECISION,
    ebpminrp_gspphot DOUBLE PRECISION,
    ebpminrp_gspphot_lower DOUBLE PRECISION,
    ebpminrp_gspphot_upper DOUBLE PRECISION,
    libname_gspphot TEXT
);

CREATE TABLE catalogdb.twomass_psc (
    ra DOUBLE PRECISION,
    decl DOUBLE PRECISION,
    err_maj REAL,
    err_min REAL,
    err_ang SMALLINT,
    j_m REAL,
    j_cmsig REAL,
    j_msigcom REAL,
    j_snr REAL,
    h_m REAL,
    h_cmsig REAL,
    h_msigcom REAL,
    h_snr REAL,
    k_m REAL,
    k_cmsig REAL,
    k_msigcom REAL,
    k_snr REAL,
    ph_qual CHARACTER(3),
    rd_flg CHARACTER(3),
    bl_flg CHARACTER(3),
    cc_flg CHARACTER(3),
    ndet CHARACTER(6),
    prox REAL,
    pxpa SMALLINT,
    pxcntr INTEGER,
    gal_contam SMALLINT,
    mp_flg SMALLINT,
    pts_key INTEGER,
    hemis CHARACTER(1),
    date DATE,
    scan SMALLINT,
    glon REAL,
    glat REAL,
    x_scan REAL,
    jdate DOUBLE PRECISION,
    j_psfchi REAL,
    h_psfchi REAL,
    k_psfchi REAL,
    j_m_stdap REAL,
    j_msig_stdap REAL,
    h_m_stdap REAL,
    h_msig_stdap REAL,
    k_m_stdap REAL,
    k_msig_stdap REAL,
    dist_edge_ns INTEGER,
    dist_edge_ew INTEGER,
    dist_edge_flg CHARACTER(2),
    dup_src SMALLINT,
    use_src SMALLINT,
    a CHARACTER(1),
    dist_opt REAL,
    phi_opt SMALLINT,
    b_m_opt REAL,
    vr_m_opt REAL,
    nopt_mchs SMALLINT,
    ext_key INTEGER,
    scan_key INTEGER,
    coadd_key INTEGER,
    coadd SMALLINT,
    designation TEXT
);

CREATE TABLE catalogdb.sdss_dr13_photoobj (
    objid BIGINT,
    run SMALLINT,
    rerun SMALLINT,
    camcol SMALLINT,
    field SMALLINT,
    obj SMALLINT,
    type SMALLINT,
    flags BIGINT,
    rowc REAL,
    rowcerr REAL,
    colc REAL,
    colcerr REAL,
    rowv REAL,
    rowverr REAL,
    colv REAL,
    colverr REAL,
    sky_u REAL,
    sky_g REAL,
    sky_r REAL,
    sky_i REAL,
    sky_z REAL,
    skyivar_u REAL,
    skyivar_g REAL,
    skyivar_r REAL,
    skyivar_i REAL,
    skyivar_z REAL,
    psfmag_u REAL,
    psfmag_g REAL,
    psfmag_r REAL,
    psfmag_i REAL,
    psfmag_z REAL,
    psfmagerr_u REAL,
    psfmagerr_g REAL,
    psfmagerr_r REAL,
    psfmagerr_i REAL,
    psfmagerr_z REAL,
    fibermag_u REAL,
    fibermag_g REAL,
    fibermag_r REAL,
    fibermag_i REAL,
    fibermag_z REAL,
    fibermagerr_u REAL,
    fibermagerr_g REAL,
    fibermagerr_r REAL,
    fibermagerr_i REAL,
    fibermagerr_z REAL,
    fiber2mag_u REAL,
    fiber2mag_g REAL,
    fiber2mag_r REAL,
    fiber2mag_i REAL,
    fiber2mag_z REAL,
    fiber2magerr_u REAL,
    fiber2magerr_g REAL,
    fiber2magerr_r REAL,
    fiber2magerr_i REAL,
    fiber2magerr_z REAL,
    petromag_u REAL,
    petromag_g REAL,
    petromag_r REAL,
    petromag_i REAL,
    petromag_z REAL,
    petromagerr_u REAL,
    petromagerr_g REAL,
    petromagerr_r REAL,
    petromagerr_i REAL,
    petromagerr_z REAL,
    psfflux_u REAL,
    psfflux_g REAL,
    psfflux_r REAL,
    psfflux_i REAL,
    psfflux_z REAL,
    psffluxivar_u REAL,
    psffluxivar_g REAL,
    psffluxivar_r REAL,
    psffluxivar_i REAL,
    psffluxivar_z REAL,
    fiberflux_u REAL,
    fiberflux_g REAL,
    fiberflux_r REAL,
    fiberflux_i REAL,
    fiberflux_z REAL,
    fiberfluxivar_u REAL,
    fiberfluxivar_g REAL,
    fiberfluxivar_r REAL,
    fiberfluxivar_i REAL,
    fiberfluxivar_z REAL,
    fiber2flux_u REAL,
    fiber2flux_g REAL,
    fiber2flux_r REAL,
    fiber2flux_i REAL,
    fiber2flux_z REAL,
    fiber2fluxivar_u REAL,
    fiber2fluxivar_g REAL,
    fiber2fluxivar_r REAL,
    fiber2fluxivar_i REAL,
    fiber2fluxivar_z REAL,
    petroflux_u REAL,
    petroflux_g REAL,
    petroflux_r REAL,
    petroflux_i REAL,
    petroflux_z REAL,
    petrofluxivar_u REAL,
    petrofluxivar_g REAL,
    petrofluxivar_r REAL,
    petrofluxivar_i REAL,
    petrofluxivar_z REAL,
    petrorad_u REAL,
    petrorad_g REAL,
    petrorad_r REAL,
    petrorad_i REAL,
    petrorad_z REAL,
    petroraderr_u REAL,
    petroraderr_g REAL,
    petroraderr_r REAL,
    petroraderr_i REAL,
    petroraderr_z REAL,
    petror50_u REAL,
    petror50_g REAL,
    petror50_r REAL,
    petror50_i REAL,
    petror50_z REAL,
    petror50err_u REAL,
    petror50err_g REAL,
    petror50err_r REAL,
    petror50err_i REAL,
    petror50err_z REAL,
    petror90_u REAL,
    petror90_g REAL,
    petror90_r REAL,
    petror90_i REAL,
    petror90_z REAL,
    petror90err_u REAL,
    petror90err_g REAL,
    petror90err_r REAL,
    petror90err_i REAL,
    petror90err_z REAL,
    mrrcc_u REAL,
    mrrcc_g REAL,
    mrrcc_r REAL,
    mrrcc_i REAL,
    mrrcc_z REAL,
    devflux_u REAL,
    devflux_g REAL,
    devflux_r REAL,
    devflux_i REAL,
    devflux_z REAL,
    devfluxivar_u REAL,
    devfluxivar_g REAL,
    devfluxivar_r REAL,
    devfluxivar_i REAL,
    devfluxivar_z REAL,
    extinction_u REAL,
    extinction_g REAL,
    extinction_r REAL,
    extinction_i REAL,
    extinction_z REAL,
    lnldev_u REAL,
    lnldev_g REAL,
    lnldev_r REAL,
    lnldev_i REAL,
    lnldev_z REAL,
    expflux_u REAL,
    expflux_g REAL,
    expflux_r REAL,
    expflux_i REAL,
    expflux_z REAL,
    expfluxivar_u REAL,
    expfluxivar_g REAL,
    expfluxivar_r REAL,
    expfluxivar_i REAL,
    expfluxivar_z REAL,
    aperflux7_u REAL,
    aperflux7_g REAL,
    aperflux7_r REAL,
    aperflux7_i REAL,
    aperflux7_z REAL,
    calibstatus_u INTEGER,
    calibstatus_g INTEGER,
    calibstatus_r INTEGER,
    calibstatus_i INTEGER,
    calibstatus_z INTEGER,
    nmgypercount_u REAL,
    nmgypercount_g REAL,
    nmgypercount_r REAL,
    nmgypercount_i REAL,
    nmgypercount_z REAL,
    resolvestatus INTEGER,
    thingid INTEGER,
    fieldid BIGINT,
    balkanid INTEGER,
    ndetect INTEGER,
    psffwhm_u REAL,
    psffwhm_g REAL,
    psffwhm_r REAL,
    psffwhm_i REAL,
    psffwhm_z REAL,
    htmid BIGINT,
    parentid BIGINT,
    specobjid NUMERIC(20,0),
    lnlstar_u REAL,
    lnlstar_g REAL,
    lnlstar_r REAL,
    lnlstar_i REAL,
    lnlstar_z REAL,
    lnlexp_u REAL,
    lnlexp_g REAL,
    lnlexp_r REAL,
    lnlexp_i REAL,
    lnlexp_z REAL,
    ra DOUBLE PRECISION,
    dec DOUBLE PRECISION,
    b DOUBLE PRECISION,
    l DOUBLE PRECISION
);

CREATE TABLE catalogdb.catalog_to_sdss_dr13_photoobj (
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
\copy catalogdb.catalog_to_sdss_dr13_photoobj FROM PROGRAM 'gzip -dc catalog_to_sdss_dr13_photoobj_primary.csv.gz' WITH CSV HEADER;
\copy catalogdb.catalog_to_twomass_psc FROM PROGRAM 'gzip -dc catalog_to_twomass_psc.csv.gz' WITH CSV HEADER;
\copy catalogdb.gaia_dr3_source FROM PROGRAM 'gzip -dc gaia_dr3_source.csv.gz' WITH CSV HEADER;
\copy catalogdb.sdss_dr13_photoobj FROM PROGRAM 'gzip -dc sdss_dr13_photoobj.csv.gz' WITH CSV HEADER;
\copy catalogdb.twomass_psc FROM PROGRAM 'gzip -dc twomass_psc.csv.gz' WITH CSV HEADER;

ALTER TABLE catalogdb.catalog ADD PRIMARY KEY (catalogid);
ALTER TABLE catalogdb.sdss_id_stacked ADD PRIMARY KEY (sdss_id);
ALTER TABLE catalogdb.catalog_to_gaia_dr3_source ADD PRIMARY KEY (catalogid, target_id, version_id);
ALTER TABLE catalogdb.catalog_to_sdss_dr13_photoobj ADD PRIMARY KEY (catalogid, target_id, version_id);
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
VACUUM ANALYZE catalogdb.catalog_to_sdss_dr13_photoobj;
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
