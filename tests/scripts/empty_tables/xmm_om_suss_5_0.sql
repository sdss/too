CREATE TABLE catalogdb.xmm_om_suss_5_0 (
    iauname text,
    n_summary bigint,
    obsid text,
    srcnum bigint,
    uvw2_srcdist real,
    uvm2_srcdist real,
    uvw1_srcdist real,
    u_srcdist real,
    b_srcdist real,
    v_srcdist real,
    ra double precision,
    "dec" double precision,
    ra_hms text,
    dec_dms text,
    poserr real,
    lii double precision,
    bii double precision,
    n_obsid integer,
    uvw2_signif real,
    uvm2_signif real,
    uvw1_signif real,
    u_signif real,
    b_signif real,
    v_signif real,
    uvw2_rate real,
    uvw2_rate_err real,
    uvm2_rate real,
    uvm2_rate_err real,
    uvw1_rate real,
    uvw1_rate_err real,
    u_rate real,
    u_rate_err real,
    b_rate real,
    b_rate_err real,
    v_rate real,
    v_rate_err real,
    uvw2_ab_flux real,
    uvw2_ab_flux_err real,
    uvm2_ab_flux real,
    uvm2_ab_flux_err real,
    uvw1_ab_flux real,
    uvw1_ab_flux_err real,
    u_ab_flux real,
    u_ab_flux_err real,
    b_ab_flux real,
    b_ab_flux_err real,
    v_ab_flux real,
    v_ab_flux_err real,
    uvw2_ab_mag real,
    uvw2_ab_mag_err real,
    uvm2_ab_mag real,
    uvm2_ab_mag_err real,
    uvw1_ab_mag real,
    uvw1_ab_mag_err real,
    u_ab_mag real,
    u_ab_mag_err real,
    b_ab_mag real,
    b_ab_mag_err real,
    v_ab_mag real,
    v_ab_mag_err real,
    uvw2_vega_mag real,
    uvw2_vega_mag_err real,
    uvm2_vega_mag real,
    uvm2_vega_mag_err real,
    uvw1_vega_mag real,
    uvw1_vega_mag_err real,
    u_vega_mag real,
    u_vega_mag_err real,
    b_vega_mag real,
    b_vega_mag_err real,
    v_vega_mag real,
    v_vega_mag_err real,
    uvw2_major_axis real,
    uvm2_major_axis real,
    uvw1_major_axis real,
    u_major_axis real,
    b_major_axis real,
    v_major_axis real,
    uvw2_minor_axis real,
    uvm2_minor_axis real,
    uvw1_minor_axis real,
    u_minor_axis real,
    b_minor_axis real,
    v_minor_axis real,
    uvw2_posang real,
    uvm2_posang real,
    uvw1_posang real,
    u_posang real,
    b_posang real,
    v_posang real,
    uvw2_quality_flag integer,
    uvm2_quality_flag integer,
    uvw1_quality_flag integer,
    u_quality_flag integer,
    b_quality_flag integer,
    v_quality_flag integer,
    uvw2_quality_flag_st text,
    uvm2_quality_flag_st text,
    uvw1_quality_flag_st text,
    u_quality_flag_st text,
    b_quality_flag_st text,
    v_quality_flag_st text,
    uvw2_extended_flag smallint,
    uvm2_extended_flag smallint,
    uvw1_extended_flag smallint,
    u_extended_flag smallint,
    b_extended_flag smallint,
    v_extended_flag smallint,
    uvw2_sky_image text,
    uvm2_sky_image text,
    uvw1_sky_image text,
    u_sky_image text,
    b_sky_image text,
    v_sky_image text,
    pk bigint NOT NULL
);