CREATE TABLE catalogdb.mastar_goodstars (
    drpver character varying(8) NOT NULL,
    mprocver character varying(8) NOT NULL,
    mangaid character varying(25) NOT NULL,
    minmjd integer NOT NULL,
    maxmjd integer NOT NULL,
    nvisits integer NOT NULL,
    nplates integer NOT NULL,
    ra double precision NOT NULL,
    "dec" double precision NOT NULL,
    epoch real NOT NULL,
    psfmag_1 real NOT NULL,
    psfmag_2 real NOT NULL,
    psfmag_3 real NOT NULL,
    psfmag_4 real NOT NULL,
    psfmag_5 real NOT NULL,
    mngtarg2 integer NOT NULL,
    input_logg real NOT NULL,
    input_teff real NOT NULL,
    input_fe_h real NOT NULL,
    input_alpha_m real NOT NULL,
    input_source character varying(16),
    photocat character varying(10) NOT NULL
);