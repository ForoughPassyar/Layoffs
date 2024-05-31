-- This is a project guided by Alex Freberg's youtube channel. 
-- Although in many cases I diverge and use different queries.

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022




SELECT * FROM new_schema.raw_layoffs;

-- I will begin with data cleaning!
-- I will check for duplicates and remove them if they exist.
-- I will standardize the data.
-- I will check for null values and see how I can fix them for the data. 
-- I will remove any columns and rows that are not needed for the analysis.






CREATE TABLE new_schema.layoffs_staging
LIKE new_schema.raw_layoffs;

INSERT layoffs_staging
SELECT * FROM new_schema.raw_layoffs;





-- Looking here for duplicates!
-- Due to the fact that this set of data doesn't have a primary key to uniquely identify each row, I will identify rows that are duplicates in the row_num column!
-- Here I just made the queries more concise than the code from the guided tutorial!






SELECT *
FROM (
SELECT company, industry, location, total_laid_off, `date`, percentage_laid_off, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
		PARTITION BY company, industry, location, total_laid_off, `date`, percentage_laid_off, stage, country, funds_raised_millions
        )
		AS row_num
FROM new_schema.layoffs_staging
) duplicates 
WHERE
	row_num > 1; 




 
    
-- Here I will check for the company Oda to confirm. These don't end up being legitimate duplicates so we have to check for the duplicates in the rest of the data!   

SELECT *
FROM new_schema. raw_layoffs
WHERE company = 'Oda'
; 




-- These are our real duplicates 

SELECT * 
FROM
( SELECT company, industry, location, total_laid_off, `date`, percentage_laid_off, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
		PARTITION BY company, industry, location, total_laid_off, `date`, percentage_laid_off, stage, country, funds_raised_millions
        )
		AS row_num
FROM new_schema.layoffs_staging
) duplicates 
WHERE
	row_num > 1; 





-- Now we want to delete the entries that have a row more than 1. 
-- Alex Freberg offered two solutions, the first which involves a temporary table where he created a table.
-- The he inserted into it a new row called row_num which he deleted those that had a row_num greater or equal to 2.
-- He also offered the approach of creating a CTE (common table expression) and then deleted duplicates.
-- From what I understand one can't delete from a temporary table.
-- I used a derived table aliased 'dupliactes' and a subquery within a delete operation.
-- I did this for readibility, and because the query was relatively simple, and wasn't being used multiple times over.






SELECT *
FROM 
( SELECT company, industry, location, total_laid_off, `date`, percentage_laid_off, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
		PARTITION BY company, industry, location, total_laid_off, `date`, percentage_laid_off, stage, country, funds_raised_millions
        )
		AS row_num
FROM new_schema.layoffs_staging
) duplicates 
WHERE
	row_num > 1; 







DELETE FROM new_schema.layoffs_staging
WHERE (company, industry, location, total_laid_off, `date`, percentage_laid_off, stage, country, funds_raised_millions) IN (
    SELECT company, industry, location, total_laid_off, `date`, percentage_laid_off, stage, country, funds_raised_millions
    FROM (
        SELECT 
            company, 
            industry, 
            location, 
            total_laid_off, 
            `date`, 
            percentage_laid_off, 
            stage, 
            country, 
            funds_raised_millions,
            ROW_NUMBER() OVER (
                PARTITION BY company, industry, location, total_laid_off, `date`, percentage_laid_off, stage, country, funds_raised_millions
                ORDER BY (SELECT NULL)
            ) AS row_num
        FROM new_schema.layoffs_staging
    ) AS duplicates
    WHERE row_num > 1
);




-- Next we can add the row_num as an additional column
-- So, here Alex Freberg adds the row_num as a column to the table.
-- I don't because at the end he basically ends up dropping the row anyways.
-- Since we already dropped the duplicayes where the row_num was greater than 1, ie. duplicates,
-- I felt it was redundent to add the row only to drop the row much later.




 
-- 2. Next, I will standardize the data.

SELECT * 
FROM new_schema.layoffs_staging;




-- Here I see some nulls and blanks within the industry column

SELECT DISTINCT industry
FROM new_schema.layoffs_staging
ORDER BY industry;



SELECT *
FROM new_schema.layoffs_staging
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;



SELECT *
FROM new_schema.layoffs_staging
WHERE company LIKE 'Bally%';

-- no issues here



SELECT *
FROM new_schema.layoffs_staging
WHERE company LIKE 'airbnb%';



-- Airbnb is a travel (industry), but this one isn't populated.
-- Alex thinks it's the same for the others.
-- Here, we write a query whereby if there is another row with the same company name,
-- It will update it to the non-null industry values to simplify it for us.
-- We don't have to manually check them all ff there were thousands .




-- Changing blanks to nulls make them easier to deal with them

UPDATE new_schema.layoffs_staging
SET industry = NULL
WHERE industry = '';




SELECT *
FROM new_schema.layoffs_staging
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;
-- All Null




-- Here we try to populate the nulls
UPDATE layoffs_staging t1
JOIN layoffs_staging t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;



    
-- Bally's was the only one without a populated row to populate this null values

SELECT *
FROM new_schema.layoffs_staging
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;




-- Crypto has multiple variations. Needs standardization

SELECT DISTINCT industry
FROM new_schema.layoffs_staging
ORDER BY industry;



UPDATE layoffs_staging
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');



SELECT DISTINCT industry
FROM new_schema.layoffs_staging
ORDER BY industry;




SELECT *
FROM new_schema.layoffs_staging;




-- We have  "United States" and some "United States." with a period at the end. Needs standardization

SELECT DISTINCT country
FROM new_schema.layoffs_staging
ORDER BY country;




UPDATE layoffs_staging
SET country = TRIM(TRAILING '.' FROM country);



-- now if we run this again it is fixed

SELECT DISTINCT country
FROM new_schema.layoffs_staging
ORDER BY country;




-- Here we fix the date columns:

SELECT *
FROM new_schema.layoffs_staging;




-- I couldn't use the str_to_date function alone since  values in the date column had duplicates.

-- So I add the REGEXP function to standardize the format of the dates.


UPDATE layoffs_staging
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y')
WHERE `date` REGEXP '[0-9]{2}/[0-9]{2}/[0-9]{4}';



-- now we can convert the data type from text to date
ALTER TABLE layoffs_staging
MODIFY COLUMN `date` DATE;




SELECT *
FROM new_schema.layoffs_staging;



-- Next, we can look at Nulls values. So, as we can see there are nulls in the total_laid_off, percentsage_laid_off, and the funds_raised_millions.
-- However, these will be easy to deal with in the EDA phase in terms of calcualtions! 
-- Isn't really anything much to change. 
-- Here I am going to remove any columns and rows that are not relevant to us!


SELECT * 
FROM new_schema.layoffs_staging
WHERE total_laid_off is NULL;



SELECT *
FROM new_schema.layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;



-- Delete data that is not really useable

DELETE FROM new_schema.layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;



-- This is where I begin the exploratory data analysis!

SELECT *
 FROM new_schema.layoffs_staging; 


 
SELECT MAX(total_laid_off)
FROM new_schema.layoffs_staging;
 -- 6650 


 
 -- We look at percentages to see how these lay offs really are
 
 SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off) 
 FROM new_schema.layoffs_staging
 WHERE percentage_laid_off IS NOT NULL; 


 
 -- Here we look for companies had a full 100 percent lay offs, probably startups
 
SELECT *
FROM new_schema.layoffs_staging
WHERE percentage_laid_off = 1;



-- Lets order them by funds_rsised_millions to see the size of these companies

SELECT *
FROM new_schema.layoffs_staging
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- So here Alex did a different analysis than I did, since his functions weren't effective in my analysis. 
-- So I am not getting necessarily the same results that he got.



-- companies with the largest layoffs.

SELECT company, total_laid_off
FROM new_schema.layoffs_staging
ORDER BY 2 DESC
LIMIT 5;
-- On a single day



-- Companies with the most total Layoffs.

SELECT company, SUM(total_laid_off)
FROM new_schema.layoffs_staging
GROUP BY company
ORDER BY 2 DeSC
LIMIT 10;



-- By location

SELECT location, SUM(total_laid_off)
FROM new_schema.layoffs_staging
GROUP BY location
ORDER BY 2 DESC
LIMIT 10; 



-- Total in the past three years or in the dataset.

SELECT country, SUM(total_laid_off) 
FROM new_schema.layoffs_staging
GROUP BY country
ORDER BY 2 DESC;



SELECT YEAR(date), SUM(total_laid_off)
FROM new_schema.layoffs_staging
GROUP BY YEAR(date)
ORDER BY 1 ASC;



SELECT industry, SUM(total_laid_off)
FROM new_schema.layoffs_staging
GROUP BY industry 
ORDER BY 2 DESC;



SELECT stage, SUM(total_laid_off)
FROM new_schema.layoffs_staging
GROUP BY stage
ORDER BY 2 DESC;



-- Most layoffs per year and ranking using CTE.

WITH company_year AS
(
	SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
    FROM layoffs_staging
    GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
	SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
    FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC; 



-- ROLLING Total of Layoffs Per Month.

SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
GROUP BY dates
ORDER BY dates ASC;





--  We are going to use this in a CTE now.


WITH DATE_CTE AS
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;

























