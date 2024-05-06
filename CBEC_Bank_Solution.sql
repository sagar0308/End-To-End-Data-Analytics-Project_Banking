USE DATABASE CZEC;
--1. What is the demographic profile of the bank's clients and how does it vary across districts?

select district_name, count(*) as total_count_per_district
from district d
inner join client c
on d.district_code = c.district_id
group by 1;

--2. How the banks have performed over the years. Give their detailed analysis year & month-wise.
--YEARWISE
SELECT YEAR(DATE) AS YEAR,
COUNT(TRANS_ID) TOTAL_TXN,
SUM(AMOUNT) TOTAL_AMT, 
FROM TRANSACTIONS
GROUP BY 1 
ORDER BY 1;

--MONTHWISE
SELECT DATE_TRUNC(MONTH,DATE) AS MONTH,
COUNT(TRANS_ID) TOTAL_TXN,
SUM(AMOUNT) TOTAL_AMT, 
FROM TRANSACTIONS
GROUP BY 1 
ORDER BY 1;

--3. What are the most common types of accounts and how do they differ in terms of usage and profitability?
SELECT * FROM TRANSACTIONS LIMIT 5;
SELECT * FROM ACCOUNT LIMIT 5;

--TOTAL NO OF CUSTOMERS AS PER ACCOUNT TYPE
SELECT DISTINCT ACCOUNT_TYPE AS ACCOUNT_TYPE, 
COUNT(*) AS COUNT_PER_ACCOUNT_TYPE
FROM ACCOUNT
GROUP BY 1;

--TOTAL NO OF TRX DONE AS PER ACCOUNT TYPE
SELECT ACCOUNT_TYPE AS ACCOUNT_TYPE,COUNT(*) AS TOTAL_TXN_COUNT_PER_BANK_TYPE
FROM TRANSACTIONS T
INNER JOIN ACCOUNT A
ON T.ACCOUNT_ID = A.ACCOUNT_ID
GROUP BY 1;

--4. Which types of cards are most frequently used by the bank's clients and what is the overall profitability of the credit card business?
SELECT * FROM TRANSACTIONS LIMIT LIMIT 10;
SELECT * FROM ACCOUNT LIMIT 10;
SELECT * FROM CARD;

SELECT CARD_ASSIGNED AS CARD_ASSIGNED,COUNT(*) AS NO_OF_TIMES_CARD_USED
FROM ACCOUNT A
INNER JOIN TRANSACTIONS T
ON A.ACCOUNT_ID = T.ACCOUNT_ID
WHERE OPERATION IN ('Withdrawal in cash','Credit card withdrawal')
GROUP BY 1
ORDER BY 2 DESC;
