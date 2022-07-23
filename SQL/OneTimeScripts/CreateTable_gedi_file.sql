-- Table: public.gedi_file

-- DROP TABLE public.gedi_file;

CREATE TABLE IF NOT EXISTS public.gedi_file
(
    filename text COLLATE pg_catalog."default" NOT NULL,
    product text COLLATE pg_catalog."default" NOT NULL,
    batch_id integer NOT NULL,
    download_url text COLLATE pg_catalog."default" NOT NULL,
    created_dttm timestamp without time zone NOT NULL DEFAULT now(),
    processed_dttm timestamp without time zone,
    is_processed integer NOT NULL DEFAULT 0,
    CONSTRAINT gedi_file_pkey PRIMARY KEY (filename, batch_id)
)

TABLESPACE pg_default;

ALTER TABLE public.gedi_file
    OWNER to postgres;