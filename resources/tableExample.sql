-- Table: amazon

-- DROP TABLE amazon;

CREATE TABLE amazon
(
  product_id character varying,
  user_d character varying,
  profile_name character varying,
  helpfulness character varying,
  score double precision,
  review_time integer,
  review_summary character varying,
  review_text text
)
WITH (
  OIDS=FALSE
);
ALTER TABLE amazon
  OWNER TO postgres;
