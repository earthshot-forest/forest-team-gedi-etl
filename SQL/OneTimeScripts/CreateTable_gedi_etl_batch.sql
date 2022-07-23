-- Table: public.gedi_etl_batch

-- DROP TABLE public.gedi_etl_batch;

CREATE TABLE IF NOT EXISTS public.gedi_etl_batch
(
    batch_id integer NOT NULL DEFAULT nextval('gedi_etl_batch_batch_id_seq'::regclass),
    product text COLLATE pg_catalog."default" NOT NULL,
    upper_left_coord text COLLATE pg_catalog."default" NOT NULL,
    lower_right_coord text COLLATE pg_catalog."default" NOT NULL,
    label text COLLATE pg_catalog."default" NOT NULL,
    crs text COLLATE pg_catalog."default" NOT NULL,
    gedi_version text COLLATE pg_catalog."default",
    do_store_file integer,
    store_path text COLLATE pg_catalog."default",
    created_dttm timestamp without time zone DEFAULT now(),
    CONSTRAINT gedi_etl_batch_pkey PRIMARY KEY (batch_id)
)

TABLESPACE pg_default;

ALTER TABLE public.gedi_etl_batch
    OWNER to postgres;