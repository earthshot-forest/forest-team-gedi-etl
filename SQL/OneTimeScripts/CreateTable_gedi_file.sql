-- Table: public.gedi_file

-- DROP TABLE public.gedi_file;

CREATE TABLE IF NOT EXISTS public.gedi_file
(
    filename text COLLATE pg_catalog."default" NOT NULL,
    product text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT gedi_file_pkey PRIMARY KEY (filename)
)

TABLESPACE pg_default;

ALTER TABLE public.gedi_file
    OWNER to earthshot;