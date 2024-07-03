-- DATA CLEANING --

create database world_layoffs;
SELECT 
    *
FROM
    layoffs;

-- 1. Remove Duplicates
-- 2. Standardize Data
-- 3. Null values or Blank values
-- 4. Remove any columns


-- 1. REMOVE DUPLICATES

Use  world_layoffs;

CREATE TABLE layoffs_staging LIKE layoffs;

SELECT 
    *
FROM
    layoffs_staging;

insert layoffs_staging
select*from layoffs;

select *, 
row_number() over (partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
as raw_num
from layoffs_staging;

with duplicate_cte as
(select *, 
row_number() over 
(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
as raw_num
from layoffs_staging)
select * from
duplicate_cte
where raw_num > 1;

SELECT 
    *
FROM
    layoffs_staging
WHERE
    company = 'casper';

CREATE TABLE `layoffs_staging2` (
    `company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` INT DEFAULT NULL,
    `percentage_laid_off` TEXT,
    `date` TEXT,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised_millions` INT DEFAULT NULL,
    `raw_num` INT
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;

SELECT 
    *
FROM
    layoffs_staging2;



insert into layoffs_staging2
select *, 
row_number() over 
(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
as raw_num
from layoffs_staging;

SELECT 
    *
FROM
    layoffs_staging2
WHERE
    raw_num > 1;

DELETE FROM layoffs_staging2 
WHERE
    raw_num > 1;

SELECT 
    *
FROM
    layoffs_staging2;


-- 2. STANDARDIZING DATA

SELECT DISTINCT
    (TRIM(company))
FROM
    layoffs_staging2;
SELECT 
    company, TRIM(company)
FROM
    layoffs_staging2;

UPDATE layoffs_staging2 
SET 
    company = TRIM(company);

SELECT DISTINCT
    industry
FROM
    layoffs_staging2
ORDER BY 1;

SELECT 
    *
FROM
    layoffs_staging2
WHERE
    industry LIKE 'crypto%';

UPDATE layoffs_staging2 
SET 
    industry = 'crypto'
WHERE
    industry LIKE 'crypto%';

SELECT DISTINCT
    location
FROM
    layoffs_staging2
ORDER BY 1;

SELECT 
    *
FROM
    layoffs_staging2
WHERE
    country LIKE 'united states%'
ORDER BY 1;

SELECT DISTINCT
    country, TRIM(TRAILING '.' FROM country)
FROM
    layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2 
SET 
    country = TRIM(TRAILING '.' FROM country)
WHERE
    country LIKE 'united states%';


SELECT 
    date
FROM
    layoffs_staging2;

SELECT 
    `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM
    layoffs_staging2;

UPDATE layoffs_staging2 
SET 
    date = STR_TO_DATE(`date`, '%m/%d/%Y');
	
alter table layoffs_staging2
modify column `date` date;


-- 3. Null values or Blank values


SELECT DISTINCT
    industry
FROM
    world_layoffs.layoffs_staging2
ORDER BY industry;


SELECT 
    *
FROM
    world_layoffs.layoffs_staging2
WHERE
    industry IS NULL OR industry = ''
ORDER BY industry;

SELECT 
    *
FROM
    world_layoffs.layoffs_staging2
WHERE
    company LIKE 'Bally%';

-- nothing wrong here

SELECT 
    *
FROM
    world_layoffs.layoffs_staging2
WHERE
    company LIKE 'airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated.

UPDATE world_layoffs.layoffs_staging2 
SET 
    industry = NULL
WHERE
    industry = '';

-- now if we check those are all null

SELECT 
    *
FROM
    world_layoffs.layoffs_staging2
WHERE
    industry IS NULL OR industry = ''
ORDER BY industry;

-- now we need to populate those nulls if possible

UPDATE layoffs_staging2 t1
        JOIN
    layoffs_staging2 t2 ON t1.company = t2.company 
SET 
    t1.industry = t2.industry
WHERE
    t1.industry IS NULL
        AND t2.industry IS NOT NULL;

SELECT 
    *
FROM
    world_layoffs.layoffs_staging2
WHERE
    industry IS NULL OR industry = ''
ORDER BY industry;

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values

SELECT DISTINCT
    industry
FROM
    world_layoffs.layoffs_staging2
ORDER BY industry;

SELECT 
    *
FROM
    world_layoffs.layoffs_staging2;

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal.
-- I like having them null because it makes it easier for calculations during the EDA phase


SELECT 
    *
FROM
    world_layoffs.layoffs_staging2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

-- 4.) Delete Useless data we can't really use


DELETE FROM world_layoffs.layoffs_staging2 
WHERE
    total_laid_off IS NULL
    AND percentage_laid_off IS NULL;

SELECT 
    *
FROM
    world_layoffs.layoffs_staging2;

