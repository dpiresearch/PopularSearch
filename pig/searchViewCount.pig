-- 
--
-- This is part of a popular search workflow that matches keywords with eventual product detail clicks
-- in order to discover popular keywords
--
-- This step loads keywords from a search_results log and product page clicks from a viewed_ad.log and
-- matches them based on a macid (cookie string)
--
--
-- Changes
--   Used skewed join for macid
--   Filter empty macids out from SRP and VIP views
--   Filter empty keywords out
--   Filter out external referers
--  

REGISTER '$deploydir/lib/pig_udf.jar'
REGISTER '$deploydir/lib/piggybank.jar'

SET default_parallel 50;

--
-- Load search results in
--
F1 = LOAD '$inputdir/search_results/$daydate/*/search_results.log*' USING PigStorage('\u0001') as (version:int,time:long,site:long,macid:chararray, clientid:chararray, catid:long, locid:long, searchstr:chararray, referer:chararray, nativereferer:int, country:chararray, language:chararray, listing_count:int);

--
-- Load page views
--
V1 = LOAD '$inputdir/viewad/$daydate/*/viewed_ad.log*' USING PigStorage('\u0001') as (version:int,time:long,site:long,adid:long,macid:chararray,usrid:long,catid:long,locid:long,ip:chararray,ua:chararray, referer:chararray, nativereferer:int, country:chararray, language:chararray);

-- Don't project ip yet
SV1 = FOREACH V1 GENERATE time, country, adid, macid, catid, locid;


-- 
-- START Section to tokenize searchstr
--

--
-- Project the keyword as well as the searchstr.  May want to project the keyword out later
--
CAT_LOC_TOK = FOREACH F1 GENERATE time, country, macid, catid, locid, clientid, SUBSTRING(searchstr,0,INDEXOF(searchstr,'|',0)) as kw, searchstr, nativereferer;


-- ***********************
-- START Filters for test
-- ***********************

-- Filter out empty keywords
CAT_LOC_TOK = FILTER CAT_LOC_TOK by (kw != 'EMPTY' AND kw != '');
-- Filter out empty cookie strings
CAT_LOC_TOK = FILTER CAT_LOC_TOK by (macid != 'EMPTY' AND macid != '' AND macid != 'UNKNOWN');

-- remove cookie that skews data
CAT_LOC_TOK = FILTER CAT_LOC_TOK by (macid != '3b812842-2930-4c1e-93c4-ef5a7094432c');
CAT_LOC_TOK = FILTER CAT_LOC_TOK by (macid != '5okictcplqj7pno3r7p425m4u4');

-- Get only clicks from within the website
CAT_LOC_TOK = FILTER CAT_LOC_TOK by (nativereferer == 1);

-- sanity check - make sure we have a reasonable number of records
-- CLTG = GROUP CAT_LOC_TOK ALL;
-- CLTGF = FOREACH CLTG GENERATE COUNT(CAT_LOC_TOK);
-- DUMP CLTGF;

-- Filter out empty macids (cookie strings) from the VIP clicks
SV1 = FILTER SV1 by (macid != 'EMPTY' AND macid != '' AND macid != 'UNKNOWN');

-- *********************
-- END Filters for test
-- *********************

--
-- Load the location id-name
--
LOCREF_MX = LOAD '/apps/hdmi-finance/kijiji/bolt/data/ref/geo_tz.csv' USING PigStorage('|') as (id:long,name:chararray);
LOCREF_CATEX = LOAD '/apps/hdmi-finance/kijiji/bolt/data/ref/geo_tz_us.csv' USING PigStorage('|') as (id:long,name:chararray);

LMX = FOREACH LOCREF_MX GENERATE id as id, name as name;
LMUS = FOREACH LOCREF_CATEX GENERATE id as id, name as name;

ALL_LOC = UNION LMX, LMUS;

-- DESCRIBE LMX;
-- DESCRIBE LMUS;

--
-- Load the category id and names
--
CATREF = LOAD '/apps/hdmi-finance/kijiji/bolt/data/ref/cat_all.csv' USING PigStorage('|') as (id:long,name:chararray);
ALL_CAT = FOREACH CATREF GENERATE id as id, name as name;

--
-- To prevent the same view from being matched with multiple searches, 
-- project the timestamp of the view into the matched relation and make sure it's distinct.
--

-- 
-- To get search-view impressions within X minutes
-- project both timestamps to the relation and make sure viewtime - searchtime < X minutes
-- TODO:  Need to figure out if the referer on the view will be a better way
-- 
SVJ = JOIN SV1 by macid, CAT_LOC_TOK by macid USING 'skewed';

--
-- Make the search-view line unique by search time, keyword, and adid
--

--
-- Project out the fields needed to get the keywords, search and view times, location and category of the ad clicked, the ad, and the search term used
--
SVJF = FOREACH SVJ GENERATE CAT_LOC_TOK::country as country, CAT_LOC_TOK::time as searchtime, SV1::time as viewtime, SV1::catid as view_catid, SV1::locid as view_locid, CAT_LOC_TOK::kw as kw, com.ebay.ecg.bolt.bigdata.pig.udf.FindAdidInSearch((chararray) SV1::adid, (chararray) CAT_LOC_TOK::searchstr) as searchit, SV1::adid as adid;


-- filter out search-view pairs that don't match on adid
SVJFF = FILTER SVJF BY (searchit != 'false');


-- filter for adview 30 minutes after search
SVJFF_30MIN = FILTER SVJFF by ((((long) viewtime - (long) searchtime) < 1800000) AND (((long) viewtime - (long) searchtime) > 0));


STORE SVJFF_30MIN INTO '$outputdir/$daydate/sv_filtered';

--
-- At this point we have have matched search and view ad within 30 minutes
--

SVJPROJ_30MIN = FOREACH SVJFF_30MIN GENERATE country, searchtime, view_catid, view_locid, kw;

SVJD = DISTINCT SVJPROJ_30MIN PARALLEL 50;

SVJ_GROUP_BY_KEYWORD = GROUP SVJD BY (country, view_catid, view_locid, kw) PARALLEL 50;
SVJ_KCOUNT = FOREACH SVJ_GROUP_BY_KEYWORD GENERATE group.$0 as country, group.$1 as catid, group.$2 as locid, group.$3 as kw, COUNT(SVJD) as kwcount;

-- DESCRIBE SVJ_KCOUNT;
-- DUMP SVJ_KCOUNT;
STORE SVJ_KCOUNT into '$outputdir/$daydate/searchview_kwcount';

