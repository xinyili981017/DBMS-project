-- 1: use user_id for connection between different join
-- 2: fuse different tables together in each join section (which table has user_id, use this as final user_id)
-- 3: we should match names between USERS and new tables
-- 4: 辨认清楚谁是main table
CREATE VIEW VIEW_USER_INFORMATION AS
SELECT A.USER_ID AS USER_ID, A.FIRST_NAME AS FIRST_NAME, A.LAST_NAME AS LAST_NAME, A.YEAR_OF_BIRTH AS YEAR_OF_BIRTH, A.MONTH_OF_BIRTH AS MONTH_OF_BIRTH, A.DAY_OF_BIRTH AS DAY_OF_BIRTH, A.GENDER AS GENDER,
        B.CURRENT_CITY AS CURRENT_CITY, B.CURRENT_STATE AS CURRENT_STATE, B.CURRENT_COUNTRY AS CURRENT_COUNTRY,
        C.HOMETOWN_CITY AS HOMETOWN_CITY, C.HOMETOWN_STATE AS HOMETOWN_STATE, C.HOMETOWN_COUNTRY AS HOMETOWN_COUNTRY,
        D.INSTITUTION_NAME AS INSTITUTION_NAME, D.PROGRAM_YEAR AS PROGRAM_YEAR, D.PROGRAM_CONCENTRATION AS PROGRAM_CONCENTRATION, D.PROGRAM_DEGREE AS PROGRAM_DEGREE
FROM USERS A
LEFT JOIN(
    SELECT USER_CURRENT_CITIES.USER_ID AS CUER_ID,
           CITIES.CITY_NAME AS CURRENT_CITY, CITIES.STATE_NAME AS CURRENT_STATE, CITIES.COUNTRY_NAME AS CURRENT_COUNTRY
    FROM USER_CURRENT_CITIES, CITIES
    WHERE USER_CURRENT_CITIES.CURRENT_CITY_ID = CITIES.CITY_ID
) B
ON B.CUER_ID = A.USER_ID
LEFT JOIN(
    SELECT USER_HOMETOWN_CITIES.USER_ID AS HUER_ID,
           CITIES.CITY_NAME AS HOMETOWN_CITY, CITIES.STATE_NAME AS HOMETOWN_STATE, CITIES.COUNTRY_NAME AS HOMETOWN_COUNTRY
    FROM USER_HOMETOWN_CITIES, CITIES
    WHERE USER_HOMETOWN_CITIES.HOMETOWN_CITY_ID = CITIES.CITY_ID
) C
ON C.HUER_ID = A.USER_ID
LEFT JOIN(
    SELECT EDUCATION.USER_ID AS EUSER_ID, EDUCATION.PROGRAM_YEAR AS PROGRAM_YEAR,
           PROGRAMS.CONCENTRATION AS PROGRAM_CONCENTRATION, PROGRAMS.INSTITUTION AS INSTITUTION_NAME, PROGRAMS.DEGREE AS PROGRAM_DEGREE
    FROM EDUCATION, PROGRAMS
    WHERE EDUCATION.PROGRAM_ID = PROGRAMS.PROGRAM_ID
) D
ON D.EUSER_ID = A.USER_ID;


CREATE VIEW VIEW_ARE_FRIENDS AS
SELECT USER1_ID, USER2_ID
FROM FRIENDS;


CREATE VIEW VIEW_PHOTO_INFORMATION AS
SELECT A.ALBUM_ID AS ALBUM_ID, A.ALBUM_OWNER_ID AS OWNER_ID, A.COVER_PHOTO_ID AS COVER_PHOTO_ID, A.ALBUM_NAME AS ALBUM_NAME, A.ALBUM_CREATED_TIME AS ALBUM_CREATED_TIME, A.ALBUM_MODIFIED_TIME AS ALBUM_MODIFIED_TIME, A.ALBUM_LINK AS ALBUM_LINK, A.ALBUM_VISIBILITY AS ALBUM_VISIBILITY,
       B.PHOTO_ID AS PHOTO_ID, B.PHOTO_CAPTION AS PHOTO_CAPTION, B.PHOTO_CREATED_TIME AS PHOTO_CREATED_TIME, B.PHOTO_MODIFIED_TIME AS PHOTO_MODIFIED_TIME, B.PHOTO_LINK AS PHOTO_LINK
FROM ALBUMS A
INNER JOIN PHOTOS B
ON A.ALBUM_ID = B.ALBUM_ID;


CREATE VIEW VIEW_EVENT_INFORMATION AS
SELECT A.EVENT_ID AS EVENT_ID, A.EVENT_CREATOR_ID AS EVENT_CREATOR_ID, A.EVENT_NAME AS EVENT_NAME, A.EVENT_TAGLINE AS EVENT_TAGLINE, A.EVENT_DESCRIPTION AS EVENT_DESCRIPTION, A.EVENT_HOST AS EVENT_HOST, A.EVENT_TYPE AS EVENT_TYPE, A.EVENT_SUBTYPE AS EVENT_SUBTYPE, A.EVENT_ADDRESS AS EVENT_ADDRESS,
       B.CITY_NAME AS EVENT_CITY, B.STATE_NAME AS EVENT_STATE, B.COUNTRY_NAME AS EVENT_COUNTRY,
       A.EVENT_START_TIME AS EVENT_START_TIME, A.EVENT_END_TIME AS EVENT_END_TIME
FROM USER_EVENTS A
INNER JOIN CITIES B
ON A.EVENT_CITY_ID = B.CITY_ID;


CREATE VIEW VIEW_TAG_INFORMATION AS
SELECT TAG_PHOTO_ID AS PHOTO_ID, TAG_SUBJECT_ID AS TAG_SUBJECT_ID, TAG_CREATED_TIME AS TAG_CREATED_TIME, TAG_X AS TAG_X_COORDINATE, TAG_Y AS TAG_Y_COORDINATE
FROM TAGS;