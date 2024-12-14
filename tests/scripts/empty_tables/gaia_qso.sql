CREATE TABLE catalogdb.gaia_qso (
    name text,
    raj2000 double precision,
    dej2000 double precision,
    e_raj2000 double precision,
    e_dej2000 double precision,
    z double precision,
    umag real,
    gmag real,
    rmag real,
    imag real,
    zmag real,
    ref text,
    w1_w2 real,
    w2_w3 real,
    w1mag real,
    pk bigint NOT NULL
);
