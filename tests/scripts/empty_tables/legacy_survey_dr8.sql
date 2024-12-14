CREATE TABLE catalogdb.legacy_survey_dr8 (
    release integer,
    brickid bigint,
    brickname text,
    objid bigint,
    type text,
    ra double precision,
    "dec" double precision,
    ra_ivar real,
    dec_ivar real,
    dchisq real[],
    ebv real,
    flux_g real,
    flux_r real,
    flux_z real,
    flux_w1 real,
    flux_w2 real,
    flux_w3 real,
    flux_w4 real,
    flux_ivar_g real,
    flux_ivar_r real,
    flux_ivar_z real,
    flux_ivar_w1 real,
    flux_ivar_w2 real,
    flux_ivar_w3 real,
    flux_ivar_w4 real,
    mw_transmission_g real,
    mw_transmission_r real,
    mw_transmission_z real,
    mw_transmission_w1 real,
    mw_transmission_w2 real,
    mw_transmission_w3 real,
    mw_transmission_w4 real,
    nobs_g integer,
    nobs_r integer,
    nobs_z integer,
    nobs_w1 integer,
    nobs_w2 integer,
    nobs_w3 integer,
    nobs_w4 integer,
    rchisq_g real,
    rchisq_r real,
    rchisq_z real,
    rchisq_w1 real,
    rchisq_w2 real,
    rchisq_w3 real,
    rchisq_w4 real,
    fracflux_g real,
    fracflux_r real,
    fracflux_z real,
    fracflux_w1 real,
    fracflux_w2 real,
    fracflux_w3 real,
    fracflux_w4 real,
    fracmasked_g real,
    fracmasked_r real,
    fracmasked_z real,
    fracin_g real,
    fracin_r real,
    fracin_z real,
    anymask_g integer,
    anymask_r integer,
    anymask_z integer,
    allmask_g integer,
    allmask_r integer,
    allmask_z integer,
    wisemask_w1 smallint,
    wisemask_w2 smallint,
    psfsize_g real,
    psfsize_r real,
    psfsize_z real,
    psfdepth_g real,
    psfdepth_r real,
    psfdepth_z real,
    galdepth_g real,
    galdepth_r real,
    galdepth_z real,
    psfdepth_w1 real,
    psfdepth_w2 real,
    wise_coadd_id text,
    fracdev real,
    fracdev_ivar real,
    shapedev_r real,
    shapedev_r_ivar real,
    shapedev_e1 real,
    shapedev_e1_ivar real,
    shapedev_e2 real,
    shapedev_e2_ivar real,
    shapeexp_r real,
    shapeexp_r_ivar real,
    shapeexp_e1 real,
    shapeexp_e1_ivar real,
    shapeexp_e2 real,
    shapeexp_e2_ivar real,
    fiberflux_g real,
    fiberflux_r real,
    fiberflux_z real,
    fibertotflux_g real,
    fibertotflux_r real,
    fibertotflux_z real,
    ref_cat text,
    ref_id bigint,
    ref_epoch real,
    gaia_phot_g_mean_mag real,
    gaia_phot_g_mean_flux_over_error real,
    gaia_phot_bp_mean_mag real,
    gaia_phot_bp_mean_flux_over_error real,
    gaia_phot_rp_mean_mag real,
    gaia_phot_rp_mean_flux_over_error real,
    gaia_astrometric_excess_noise real,
    gaia_duplicated_source boolean,
    gaia_phot_bp_rp_excess_factor real,
    gaia_astrometric_sigma5d_max real,
    gaia_astrometric_params_solved smallint,
    parallax real,
    parallax_ivar real,
    pmra real,
    pmra_ivar real,
    pmdec real,
    pmdec_ivar real,
    maskbits integer,
    ls_id bigint NOT NULL,
    tycho_ref bigint,
    gaia_sourceid bigint
);
