CREATE TABLE catalogdb.catalog_to_sdss_dr19p_speclite (
    catalogid bigint NOT NULL,
    target_id character varying(255) NOT NULL,
    version_id smallint NOT NULL,
    distance double precision,
    best boolean NOT NULL,
    plan_id text,
    added_by_phase smallint
);