use world_layoffs;
--showing the data type in the table
DESCRIBE layoffs_staging2;
-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;
INSERT layoffs_staging 
SELECT * 
FROM world_layoffs.layoffs;

SELECT *,
row_number() over(
partition by company, industry,total_laid_off,percentage_laid_off,"date",location,stage,country,funds_raised_millions) as row_num
FROM layoffs_staging;
with duplicate_cte as
(
SELECT *,
row_number() over(
partition by company, industry,total_laid_off,percentage_laid_off,"date",location,stage,country,funds_raised_millions) as row_num
FROM layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;
--show all duplicate
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;
--clear all duplicate
DELETE 
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

-- 2. standardize data and fix errors
--clear the white space using trim function
select company , trim(company)
from layoffs_staging2
limit 10;
update layoffs_staging2
set company =trim(company);
select distinct industry
from layoffs_staging2
order by 1;
--grouping all by "crypto"
select *
from layoffs_staging2
where industry like "crypto%";
update layoffs_staging2
set industry = "crypto"
where industry like "crypto%";
select *
from layoffs_staging2
where industry like "crypto%";
-- analysing the country coloumn
select distinct country
from layoffs_staging2
order by 1;
-- cleaning "." from "United States" colomn
update layoffs_staging2
set country = "United States"
where country like "United States%";
--method 2
update layoffs_staging2
set country = trim(trailing "." from country)
where country like "United States%";
--grouping all by "Austria"
update layoffs_staging2
set country = "Austria"
where country like "Austr%";
--changing the date from the text format to date format
update layoffs_staging2
set date = str_to_date (date,"%m/%d/%Y");
-- 3. Look at null values and see what 
-- to see the null and blanck values in the (industry, total_laid_off, percentage_laid_off)
select  *
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;
select *
from layoffs_staging2
where industry is null 
or industry=""
order by 1;
-- to see the null and not null in the industry 
select *
from layoffs_staging2 t1
join layoffs_staging2 t2
    on t1.company=t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry="")
and t2.industry is not null;
--changing "" to null
update layoffs_staging2 
set industry= null
where industry = "";
-- updating the null values with the true values
update layoffs_staging2 t1
join layoffs_staging2 t2
    on t1.company=t2.company
    and t1.location = t2.location
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;
--testing the airbnb company
select *
from layoffs_staging2
where company = "Airbnb";
-- 4. remove any columns and rows that are not necessary - few ways
