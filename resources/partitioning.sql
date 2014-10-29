-- SQL to create 18 years partitions and triggers for amazon reviews
-- 
--


CREATE TABLE products (
    product_id   character varying,
    title        character varying,
    price        character varying
);

-- create base table from which all partionions will inherit
CREATE TABLE users (
	user_id		character varying,
	product_id 	character varying,
	profile_name character varying,
	score double precision,
	review_summary character varying,
	review_time integer,
	review_text text
);

-- create partitions based on all the years from where data set starts
CREATE TABLE users_y1995 (
	CHECK ( extract(year from (timestamp with time zone 'epoch' + review_time * interval '1 second') = 1995)
) INHERITS (users);
CREATE TABLE users_y1996 (
	CHECK ( extract(year from (timestamp with time zone 'epoch' + review_time * interval '1 second') = 1996)
) INHERITS (users);
CREATE TABLE users_y1997 (
	CHECK ( extract(year from (timestamp with time zone 'epoch' + review_time * interval '1 second') = 1997)
) INHERITS (users);
CREATE TABLE users_y1998 (
	CHECK ( extract(year from (timestamp with time zone 'epoch' + review_time * interval '1 second') = 1998)
) INHERITS (users);
CREATE TABLE users_y1999 (
	CHECK ( extract(year from (timestamp with time zone 'epoch' + review_time * interval '1 second') = 1999)
) INHERITS (users);
CREATE TABLE users_y2000 (
	CHECK ( extract(year from (timestamp with time zone 'epoch' + review_time * interval '1 second') = 2000)
) INHERITS (users);
CREATE TABLE users_y2001 (
	CHECK ( extract(year from (timestamp with time zone 'epoch' + review_time * interval '1 second') = 2001)
) INHERITS (users);
CREATE TABLE users_y2002 (
	CHECK ( extract(year from (timestamp with time zone 'epoch' + review_time * interval '1 second') = 2002)
) INHERITS (users);
CREATE TABLE users_y2003 (
	CHECK ( extract(year from (timestamp with time zone 'epoch' + review_time * interval '1 second') = 2003)
) INHERITS (users);
CREATE TABLE users_y2004 (
	CHECK ( extract(year from (timestamp with time zone 'epoch' + review_time * interval '1 second') = 2004)
) INHERITS (users);
CREATE TABLE users_y2005 (
	CHECK ( extract(year from (timestamp with time zone 'epoch' + review_time * interval '1 second') = 2005)
) INHERITS (users);
CREATE TABLE users_y2006 (
	CHECK ( extract(year from (timestamp with time zone 'epoch' + review_time * interval '1 second') = 2006)
) INHERITS (users);
CREATE TABLE users_y2007 (
	CHECK ( extract(year from (timestamp with time zone 'epoch' + review_time * interval '1 second') = 2007)
) INHERITS (users);
CREATE TABLE users_y2008 (
	CHECK ( extract(year from (timestamp with time zone 'epoch' + review_time * interval '1 second') = 2008)
) INHERITS (users);
CREATE TABLE users_y2009 (
	CHECK ( extract(year from (timestamp with time zone 'epoch' + review_time * interval '1 second') = 2009)
) INHERITS (users);
CREATE TABLE users_y2010 (
	CHECK ( extract(year from (timestamp with time zone 'epoch' + review_time * interval '1 second') = 2010)
) INHERITS (users);
CREATE TABLE users_y2011 (
	CHECK ( extract(year from (timestamp with time zone 'epoch' + review_time * interval '1 second') = 2011)
) INHERITS (users);
CREATE TABLE users_y2012 (
	CHECK ( extract(year from (timestamp with time zone 'epoch' + review_time * interval '1 second') = 2012)
) INHERITS (users);
CREATE TABLE users_y2013 (
	CHECK ( extract(year from (timestamp with time zone 'epoch' + review_time * interval '1 second') = 2013)
) INHERITS (users);
CREATE TABLE users_y2014 (
	CHECK ( extract(year from (timestamp with time zone 'epoch' + review_time * interval '1 second') = 2014)
) INHERITS (users);

-- create indexes on review_time column
CREATE INDEX i_users_y1995 ON users_y1995 (review_time);
CREATE INDEX i_users_y1996 ON users_y1995 (review_time);
CREATE INDEX i_users_y1997 ON users_y1995 (review_time);
CREATE INDEX i_users_y1998 ON users_y1995 (review_time);
CREATE INDEX i_users_y1999 ON users_y1995 (review_time);
CREATE INDEX i_users_y2000 ON users_y1995 (review_time);
CREATE INDEX i_users_y2001 ON users_y1995 (review_time);
CREATE INDEX i_users_y2002 ON users_y1995 (review_time);
CREATE INDEX i_users_y2003 ON users_y1995 (review_time);
CREATE INDEX i_users_y2004 ON users_y1995 (review_time);
CREATE INDEX i_users_y2005 ON users_y1995 (review_time);
CREATE INDEX i_users_y2006 ON users_y1995 (review_time);
CREATE INDEX i_users_y2007 ON users_y1995 (review_time);
CREATE INDEX i_users_y2008 ON users_y1995 (review_time);
CREATE INDEX i_users_y2009 ON users_y1995 (review_time);
CREATE INDEX i_users_y2010 ON users_y1995 (review_time);
CREATE INDEX i_users_y2011 ON users_y1995 (review_time);
CREATE INDEX i_users_y2012 ON users_y1995 (review_time);
CREATE INDEX i_users_y2013 ON users_y1995 (review_time);
CREATE INDEX i_users_y2014 ON users_y1995 (review_time);

-- create trigger for inserting into current year function
CREATE OR REPLACE FUNCTION reviews_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO users_y2014 VALUES (NEW.*);
    RETURN NULL;
END;
$$
LANGUAGE plpgsql;

-- create trigger to use the function
CREATE TRIGGER insert_reviews_trigger
    BEFORE INSERT ON users
    FOR EACH ROW EXECUTE PROCEDURE reviews_insert();

-- check partitioned tables cumulative query
SET constraint_exclusion = on;
SELECT count(*) FROM users;
