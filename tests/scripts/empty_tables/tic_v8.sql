CREATE TABLE catalogdb.tic_v8 (
    id bigint NOT NULL,
    version character varying(8),
    hip integer,
    tyc character varying(12),
    ucac character varying(10),
    twomass character varying(20),
    sdss bigint,
    allwise character varying(20),
    gaia character varying(20),
    apass character varying(30),
    kic integer,
    objtype character varying(16),
    typesrc character varying(16),
    ra double precision,
    "dec" double precision,
    posflag character varying(12),
    pmra real,
    e_pmra real,
    pmdec real,
    e_pmdec real,
    pmflag character varying(12),
    plx real,
    e_plx real,
    parflag character varying(12),
    gallong double precision,
    gallat double precision,
    eclong double precision,
    eclat double precision,
    bmag real,
    e_bmag real,
    vmag real,
    e_vmag real,
    umag real,
    e_umag real,
    gmag real,
    e_gmag real,
    rmag real,
    e_rmag real,
    imag real,
    e_imag real,
    zmag real,
    e_zmag real,
    jmag real,
    e_jmag real,
    hmag real,
    e_hmag real,
    kmag real,
    e_kmag real,
    twomflag character varying(20),
    prox real,
    w1mag real,
    e_w1mag real,
    w2mag real,
    e_w2mag real,
    w3mag real,
    e_w3mag real,
    w4mag real,
    e_w4mag real,
    gaiamag real,
    e_gaiamag real,
    tmag real,
    e_tmag real,
    tessflag character varying(20),
    spflag character varying(20),
    teff real,
    e_teff real,
    logg real,
    e_logg real,
    mh real,
    e_mh real,
    rad real,
    e_rad real,
    mass real,
    e_mass real,
    rho real,
    e_rho real,
    lumclass character varying(10),
    lum real,
    e_lum real,
    d real,
    e_d real,
    ebv real,
    e_ebv real,
    numcont integer,
    contratio real,
    disposition character varying(10),
    duplicate_id bigint,
    priority real,
    eneg_ebv real,
    epos_ebv real,
    ebvflag character varying(20),
    eneg_mass real,
    epos_mass real,
    eneg_rad real,
    epos_rad real,
    eneg_rho real,
    epos_rho real,
    eneg_logg real,
    epos_logg real,
    eneg_lum real,
    epos_lum real,
    eneg_dist real,
    epos_dist real,
    distflag character varying(20),
    eneg_teff real,
    epos_teff real,
    tefflag character varying(20),
    gaiabp real,
    e_gaiabp real,
    gaiarp real,
    e_gaiarp real,
    gaiaqflag integer,
    starchareflag character varying(20),
    vmagflag character varying(20),
    bmagflag character varying(20),
    splits character varying(20),
    e_ra double precision,
    e_dec double precision,
    ra_orig double precision,
    dec_orig double precision,
    e_ra_orig double precision,
    e_dec_orig double precision,
    raddflag integer,
    wdflag integer,
    objid bigint,
    gaia_int bigint,
    twomass_psc text,
    twomass_psc_pts_key integer,
    tycho2_tycid integer,
    allwise_cntr bigint
);

CREATE MATERIALIZED VIEW catalogdb.tic_v8_extended AS
 SELECT tic_v8.id,
    tic_v8.version,
    tic_v8.hip,
    tic_v8.tyc,
    tic_v8.ucac,
    tic_v8.twomass,
    tic_v8.sdss,
    tic_v8.allwise,
    tic_v8.gaia,
    tic_v8.apass,
    tic_v8.kic,
    tic_v8.objtype,
    tic_v8.typesrc,
    tic_v8.ra,
    tic_v8."dec",
    tic_v8.posflag,
    tic_v8.pmra,
    tic_v8.e_pmra,
    tic_v8.pmdec,
    tic_v8.e_pmdec,
    tic_v8.pmflag,
    tic_v8.plx,
    tic_v8.e_plx,
    tic_v8.parflag,
    tic_v8.gallong,
    tic_v8.gallat,
    tic_v8.eclong,
    tic_v8.eclat,
    tic_v8.bmag,
    tic_v8.e_bmag,
    tic_v8.vmag,
    tic_v8.e_vmag,
    tic_v8.umag,
    tic_v8.e_umag,
    tic_v8.gmag,
    tic_v8.e_gmag,
    tic_v8.rmag,
    tic_v8.e_rmag,
    tic_v8.imag,
    tic_v8.e_imag,
    tic_v8.zmag,
    tic_v8.e_zmag,
    tic_v8.jmag,
    tic_v8.e_jmag,
    tic_v8.hmag,
    tic_v8.e_hmag,
    tic_v8.kmag,
    tic_v8.e_kmag,
    tic_v8.twomflag,
    tic_v8.prox,
    tic_v8.w1mag,
    tic_v8.e_w1mag,
    tic_v8.w2mag,
    tic_v8.e_w2mag,
    tic_v8.w3mag,
    tic_v8.e_w3mag,
    tic_v8.w4mag,
    tic_v8.e_w4mag,
    tic_v8.gaiamag,
    tic_v8.e_gaiamag,
    tic_v8.tmag,
    tic_v8.e_tmag,
    tic_v8.tessflag,
    tic_v8.spflag,
    tic_v8.teff,
    tic_v8.e_teff,
    tic_v8.logg,
    tic_v8.e_logg,
    tic_v8.mh,
    tic_v8.e_mh,
    tic_v8.rad,
    tic_v8.e_rad,
    tic_v8.mass,
    tic_v8.e_mass,
    tic_v8.rho,
    tic_v8.e_rho,
    tic_v8.lumclass,
    tic_v8.lum,
    tic_v8.e_lum,
    tic_v8.d,
    tic_v8.e_d,
    tic_v8.ebv,
    tic_v8.e_ebv,
    tic_v8.numcont,
    tic_v8.contratio,
    tic_v8.disposition,
    tic_v8.duplicate_id,
    tic_v8.priority,
    tic_v8.eneg_ebv,
    tic_v8.epos_ebv,
    tic_v8.ebvflag,
    tic_v8.eneg_mass,
    tic_v8.epos_mass,
    tic_v8.eneg_rad,
    tic_v8.epos_rad,
    tic_v8.eneg_rho,
    tic_v8.epos_rho,
    tic_v8.eneg_logg,
    tic_v8.epos_logg,
    tic_v8.eneg_lum,
    tic_v8.epos_lum,
    tic_v8.eneg_dist,
    tic_v8.epos_dist,
    tic_v8.distflag,
    tic_v8.eneg_teff,
    tic_v8.epos_teff,
    tic_v8.tefflag,
    tic_v8.gaiabp,
    tic_v8.e_gaiabp,
    tic_v8.gaiarp,
    tic_v8.e_gaiarp,
    tic_v8.gaiaqflag,
    tic_v8.starchareflag,
    tic_v8.vmagflag,
    tic_v8.bmagflag,
    tic_v8.splits,
    tic_v8.e_ra,
    tic_v8.e_dec,
    tic_v8.ra_orig,
    tic_v8.dec_orig,
    tic_v8.e_ra_orig,
    tic_v8.e_dec_orig,
    tic_v8.raddflag,
    tic_v8.wdflag,
    tic_v8.objid,
    tic_v8.gaia_int,
    tic_v8.twomass_psc,
    tic_v8.twomass_psc_pts_key,
    tic_v8.tycho2_tycid,
    tic_v8.allwise_cntr
   FROM catalogdb.tic_v8
  WHERE ((tic_v8.objtype)::text = 'EXTENDED'::text)
  WITH NO DATA;
