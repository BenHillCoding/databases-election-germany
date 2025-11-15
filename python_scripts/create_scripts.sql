-- Table: public.bundesland

-- DROP TABLE IF EXISTS public.bundesland;

CREATE TABLE IF NOT EXISTS public.bundesland
(
    id integer NOT NULL,
    name text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT bundesland_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.bundesland
    OWNER to postgres;

-- Table: public.direktkandidatur

-- DROP TABLE IF EXISTS public.direktkandidatur;

CREATE TABLE IF NOT EXISTS public.direktkandidatur
(
    id integer NOT NULL DEFAULT nextval('direktkandidatur_id_seq'::regclass),
    kandidat_id integer NOT NULL,
    wahl_id integer NOT NULL,
    wahlkreis_id integer NOT NULL,
    partei_id integer NOT NULL,
    CONSTRAINT direktkandidatur_pkey PRIMARY KEY (id),
    CONSTRAINT kandidat_id FOREIGN KEY (kandidat_id)
        REFERENCES public.kandidat (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,
    CONSTRAINT partei_id FOREIGN KEY (partei_id)
        REFERENCES public.partei (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,
    CONSTRAINT wahl_id FOREIGN KEY (wahl_id)
        REFERENCES public.wahl (nummer) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,
    CONSTRAINT wahlkreis_id FOREIGN KEY (wahlkreis_id)
        REFERENCES public.wahlkreis (nummer) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.direktkandidatur
    OWNER to postgres;

-- Table: public.kandidat

-- DROP TABLE IF EXISTS public.kandidat;

CREATE TABLE IF NOT EXISTS public.kandidat
(
    id integer NOT NULL DEFAULT nextval('kandidat_id_seq'::regclass),
    vorname text COLLATE pg_catalog."default" NOT NULL,
    nachname text COLLATE pg_catalog."default" NOT NULL,
    geburtsjahr integer NOT NULL,
    partei_id integer NOT NULL,
    CONSTRAINT kandidat_pkey PRIMARY KEY (id),
    CONSTRAINT partei FOREIGN KEY (partei_id)
        REFERENCES public.partei (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.kandidat
    OWNER to postgres;

-- Table: public.landesliste

-- DROP TABLE IF EXISTS public.landesliste;

CREATE TABLE IF NOT EXISTS public.landesliste
(
    id integer NOT NULL DEFAULT nextval('landesliste_id_seq'::regclass),
    wahl_id integer NOT NULL,
    bundesland_id integer NOT NULL,
    partei_id integer NOT NULL,
    CONSTRAINT landesliste_pkey PRIMARY KEY (id),
    CONSTRAINT bundesland_id FOREIGN KEY (bundesland_id)
        REFERENCES public.bundesland (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,
    CONSTRAINT partei_id FOREIGN KEY (partei_id)
        REFERENCES public.partei (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,
    CONSTRAINT wahl_id FOREIGN KEY (wahl_id)
        REFERENCES public.wahl (nummer) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.landesliste
    OWNER to postgres;

-- Table: public.listenplatz

-- DROP TABLE IF EXISTS public.listenplatz;

CREATE TABLE IF NOT EXISTS public.listenplatz
(
    id integer NOT NULL DEFAULT nextval('listenplatz_id_seq'::regclass),
    landesliste_id integer NOT NULL,
    kandidat_id integer NOT NULL,
    "position" integer NOT NULL,
    CONSTRAINT listenplatz_pkey PRIMARY KEY (id),
    CONSTRAINT kandidat_id FOREIGN KEY (kandidat_id)
        REFERENCES public.kandidat (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,
    CONSTRAINT landesliste_id FOREIGN KEY (landesliste_id)
        REFERENCES public.landesliste (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.listenplatz
    OWNER to postgres;

-- Table: public.partei

-- DROP TABLE IF EXISTS public.partei;

CREATE TABLE IF NOT EXISTS public.partei
(
    id integer NOT NULL DEFAULT nextval('partei_id_seq'::regclass),
    name text COLLATE pg_catalog."default" NOT NULL,
    nationale_minderheit boolean NOT NULL DEFAULT false,
    kuerzel text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT partei_pkey PRIMARY KEY (id),
    CONSTRAINT unique_kuerzel UNIQUE (kuerzel)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.partei
    OWNER to postgres;

-- Table: public.wahl

-- DROP TABLE IF EXISTS public.wahl;

CREATE TABLE IF NOT EXISTS public.wahl
(
    nummer numeric NOT NULL,
    datum date NOT NULL,
    CONSTRAINT "Wahl_pkey" PRIMARY KEY (nummer)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.wahl
    OWNER to postgres;

-- Table: public.wahlkreis

-- DROP TABLE IF EXISTS public.wahlkreis;

CREATE TABLE IF NOT EXISTS public.wahlkreis
(
    nummer integer NOT NULL,
    name text COLLATE pg_catalog."default" NOT NULL,
    bundesland_id integer NOT NULL,
    CONSTRAINT wahlkreis_pkey PRIMARY KEY (nummer),
    CONSTRAINT bundesland FOREIGN KEY (bundesland_id)
        REFERENCES public.bundesland (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.wahlkreis
    OWNER to postgres;

-- Table: public.wahlkreisergebnis

-- DROP TABLE IF EXISTS public.wahlkreisergebnis;

CREATE TABLE IF NOT EXISTS public.wahlkreisergebnis
(
    id integer NOT NULL DEFAULT nextval('wahlkreisergebnis_id_seq'::regclass),
    wahl_id integer NOT NULL,
    wahlkreis_id integer NOT NULL,
    wahlberechtigte integer,
    CONSTRAINT wahlkreisergebnis_pkey PRIMARY KEY (id),
    CONSTRAINT wahl_id FOREIGN KEY (wahl_id)
        REFERENCES public.wahl (nummer) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,
    CONSTRAINT wahlkreis_id FOREIGN KEY (wahlkreis_id)
        REFERENCES public.wahlkreis (nummer) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.wahlkreisergebnis
    OWNER to postgres;

-- Table: public.erststimme

-- DROP TABLE IF EXISTS public.erststimme;

CREATE TABLE IF NOT EXISTS public.erststimme
(
    id integer,
    wahlkreisergebnis_id integer,
    gueltig boolean,
    direktkandidatur_id integer
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.erststimme
    OWNER to postgres;

-- Table: public.zweitstimme

-- DROP TABLE IF EXISTS public.zweitstimme;

CREATE TABLE IF NOT EXISTS public.zweitstimme
(
    id integer,
    wahlkreisergebnis_id integer,
    gueltig boolean,
    partei_id integer
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.zweitstimme
    OWNER to postgres;