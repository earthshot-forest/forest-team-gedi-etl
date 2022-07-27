-- Table: public.gedi_load_log

-- DROP TABLE public.gedi_load_log;

CREATE TABLE IF NOT EXISTS public.gedi_load_log
(
    id integer NOT NULL DEFAULT nextval('gedi_load_log_id_seq'::regclass),
    log_level text COLLATE pg_catalog."default" NOT NULL,
    message text COLLATE pg_catalog."default" NOT NULL,
    exception text COLLATE pg_catalog."default",
    filename text COLLATE pg_catalog."default",
    created_dttm timestamp without time zone DEFAULT now(),
    CONSTRAINT gedi_load_log_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE public.gedi_load_log
    OWNER to postgres;