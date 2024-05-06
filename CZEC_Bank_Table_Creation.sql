CREATE OR REPLACE DATABASE CZEC;
USE DATABASE CZEC;

CREATE OR REPLACE TABLE DISTRICT(
District_Code INT PRIMARY KEY	,
District_Name VARCHAR(100)	,
Region VARCHAR(100)	,
No_of_inhabitants	INT,
No_of_municipalities_with_inhabitants_less_499 INT,
No_of_municipalities_with_inhabitants_500_btw_1999	INT,
No_of_municipalities_with_inhabitants_2000_btw_9999	INT,
No_of_municipalities_with_inhabitants_less_10000 INT,	
No_of_cities	INT,
Ratio_of_urban_inhabitants	FLOAT,
Average_salary	INT,
No_of_entrepreneurs_per_1000_inhabitants INT,
No_committed_crime_2017	INT,
No_committed_crime_2018 INT
) ;

CREATE OR REPLACE TABLE ACCOUNT(
account_id INT PRIMARY KEY,
district_id	INT,
frequency	VARCHAR(40),
Date DATE ,
Account_Type VARCHAR(100) ,
Card_Assigned VARCHAR(20),
FOREIGN KEY (district_id) references DISTRICT(District_Code) 
);

CREATE OR REPLACE TABLE ORDER_LIST (
order_id	INT PRIMARY KEY,
account_id	INT,
bank_to	VARCHAR(45),
account_to	INT,
amount FLOAT,
FOREIGN KEY (account_id) references ACCOUNT(account_id)
);



CREATE OR REPLACE TABLE LOAN(
loan_id	INT ,
account_id	INT,
Date	DATE,
amount	INT,
duration	INT,
payments	INT,
status VARCHAR(35),
FOREIGN KEY (account_id) references ACCOUNT(account_id)
);



CREATE OR REPLACE TABLE TRANSACTIONS(
trans_id INT,	
account_id	INT,
Date	DATE,
Type	VARCHAR(30),
operation	VARCHAR(40),
amount	INT,
balance	FLOAT,
Purpose	VARCHAR(40),
bank	VARCHAR(45),
account_partner_id INT,
FOREIGN KEY (account_id) references ACCOUNT(account_id));


CREATE OR REPLACE TABLE CLIENT(
client_id	INT PRIMARY KEY,
Sex	CHAR(10),
Birth_date	DATE,
district_id INT,
FOREIGN KEY (district_id) references DISTRICT(District_Code) 
);


CREATE OR REPLACE TABLE DISPOSITION(
disp_id	INT PRIMARY KEY,
client_id INT,
account_id	INT,
type CHAR(15),
FOREIGN KEY (account_id) references ACCOUNT(account_id),
FOREIGN KEY (client_id) references CLIENT(client_id)
);


CREATE OR REPLACE TABLE CARD(
card_id	INT PRIMARY KEY,
disp_id	INT,
type CHAR(10)	,
issued DATE,
FOREIGN KEY (disp_id) references DISPOSITION(disp_id)
);

--------------------------------------------------------------------------------
CREATE OR REPLACE STORAGE INTEGRATION bank_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::339712734802:role/czecbank_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://czecbank0308/');

desc integration bank_int;

create or replace file format czec_csv
    type = 'csv' 
    compression = 'none' 
    field_delimiter = ','
    field_optionally_enclosed_by = 'none'
    skip_header = 1 ;  

CREATE OR REPLACE STAGE CZEC_STAGE
url = 's3://czecbank0308 '
file_format = CZEC_csv
storage_integration = bank_int;

LIST @CZEC_STAGE;
SHOW STAGES;

CREATE OR REPLACE PIPE ACCOUNT_SNOWPIPE AUTO_INGEST = TRUE AS
COPY INTO CZEC.PUBLIC.ACCOUNT
FROM '@CZEC_STAGE/ACCOUNT/'
FILE_FORMAT = czec_csv;

CREATE OR REPLACE PIPE CARD_SNOWPIPE AUTO_INGEST = TRUE AS
COPY INTO CZEC.PUBLIC.CARD
FROM '@CZEC_STAGE/CARD/'
FILE_FORMAT = czec_csv;

CREATE OR REPLACE PIPE CLIENT_SNOWPIPE AUTO_INGEST = TRUE AS
COPY INTO CZEC.PUBLIC.CLIENT
FROM '@CZEC_STAGE/CLIENT/'
FILE_FORMAT = czec_csv;

CREATE OR REPLACE PIPE DISPOSITION_SNOWPIPE AUTO_INGEST = TRUE AS
COPY INTO CZEC.PUBLIC.DISPOSITION
FROM '@CZEC_STAGE/DISPOSITION/'
FILE_FORMAT = czec_csv;

CREATE OR REPLACE PIPE DISTRICT_SNOWPIPE AUTO_INGEST = TRUE AS
COPY INTO CZEC.PUBLIC.DISTRICT
FROM '@CZEC_STAGE/DISTRICT/'
FILE_FORMAT = czec_csv;

CREATE OR REPLACE PIPE LOAN_SNOWPIPE AUTO_INGEST = TRUE AS
COPY INTO CZEC.PUBLIC.LOAN
FROM '@CZEC_STAGE/LOAN/'
FILE_FORMAT = czec_csv;

CREATE OR REPLACE PIPE ORDER_LIST_SNOWPIPE AUTO_INGEST = TRUE AS
COPY INTO CZEC.PUBLIC.ORDER_LIST
FROM '@CZEC_STAGE/ORDER_LIST/'
FILE_FORMAT = czec_csv;


CREATE OR REPLACE PIPE TRANSACTIONS_SNOWPIPE AUTO_INGEST = TRUE AS
COPY INTO CZEC.PUBLIC.TRANSACTIONS
FROM '@CZEC_STAGE/TRANSACTIONS/'
FILE_FORMAT = czec_csv;

SHOW PIPES;
ALTER PIPE ACCOUNT_SNOWPIPE REFRESH;
ALTER PIPE CARD_SNOWPIPE REFRESH;
ALTER PIPE CLIENT_SNOWPIPE REFRESH;
ALTER PIPE DISPOSITION_SNOWPIPE REFRESH;
ALTER PIPE DISTRICT_SNOWPIPE REFRESH;
ALTER PIPE LOAN_SNOWPIPE REFRESH;
ALTER PIPE ORDER_LIST_SNOWPIPE REFRESH;
ALTER PIPE TRANSACTIONS_SNOWPIPE REFRESH;

select count(*) from account;
select count(*) from card;
select count(*) from client;
select count(*) from disposition;
select count(*) from district;
select count(*) from loan;
select count(*) from order_list;
select count(*) from TRANSACTIONS;


CREATE OR REPLACE PROCEDURE TRANSACTION_LATEST()
RETURNS STRING 
LANGUAGE SQL
AS
$$
CREATE OR REPLACE TABLE TRANSACTIONS_LATEST(
trans_id INT,	
account_id	INT,
Date	DATE,
Type	VARCHAR(30),
operation	VARCHAR(40),
amount	INT,
balance	FLOAT,
Purpose	VARCHAR(40),
bank	VARCHAR(45),
account_partner_id INT,
FOREIGN KEY (account_id) references ACCOUNT(account_id));
$$;

SHOW PROCEDURES;

CREATE OR REPLACE TASK TRANSACTIONS_LATEST_TASK
WAREHOUSE = MEDIUM
SCHEDULE = '5 MINUTE'
AS CALL TRANSACTION_LATEST();


CREATE OR REPLACE PROCEDURE TRANSACTION_ALL_DATA()
RETURNS STRING 
LANGUAGE SQL
AS
$$
INSERT INTO TRANSACTIONS
SELECT * FROM TRANSACTIONS_LATEST;
$$;


SHOW PROCEDURES;
CREATE OR REPLACE TASK TRANSACTIONS_ALL_DATA
WAREHOUSE = MEDIUM
SCHEDULE = '8 MINUTE'
AS CALL TRANSACTION_ALL_DATA();


ALTER TASK TRANSACTIONS_LATEST_TASK RESUME;
ALTER TASK TRANSACTIONS_LATEST_TASK SUSPEND;

ALTER TASK TRANSACTIONS_ALL_DATA RESUME;
ALTER TASK TRANSACTIONS_ALL_DATA SUSPEND;
