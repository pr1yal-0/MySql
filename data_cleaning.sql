select *
from world_layoffs.layoffs;

-- 1. remove duplicate

SET SQL_SAFE_UPDATES = 0;


create table layoffs_staging
like layoffs;

select *
from world_layoffs.layoffs_staging;
insert layoffs_staging
select *
from layoffs;

select *,
row_number() over(partition by
company, industry, total_laid_off, percentage_laid_off, 'date') as row_num
from layoffs_staging;

with duplicate_cte as
(
select *,
row_number() over(partition by
company, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

select *
from layoffs_staging
where company = 'casper'; 



CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
 
select *
from layoffs_staging2
where row_num > 1;
insert into layoffs_staging2
select *,
row_number() over(partition by
company, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoffs_staging;


delete
from layoffs_staging2
where row_num > 1;
select *
from layoffs_staging2;

-- 2. standardize the data

select company, trim(company)
from layoffs_staging2;
UPDATE layoffs_staging2
SET company = TRIM(company);

select *
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'crypto'
where industry like 'crypto%'; 

select distinct country , trim(trailing '.' from country)
from layoffs_staging2
order by 1;
update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States';
select *
from layoffs_staging2;
select `date` , 
str_to_date(`date` , '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date` , '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;
select *
from layoffs_staging2;


-- 3. null values or blank values

select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

update layoffs_staging2
set industry = null
where industry = '';


select *
from layoffs_staging2
where industry is null
or industry = '';
select *
from layoffs_staging2
where company = 'Balley%';

select *
from layoffs_staging2 st2
join layoffs_staging2 t2
	on st2.company = t2.company
	and st2.location = t2.location
where (st2.industry is null)
and t2.industry is not null;

update layoffs_staging2 st2
join layoffs_staging2 t2
	on st2.company = t2.company
    set st2.industry = t2.industry
where (st2.industry is null or st2.industry = '')
and t2.industry is not null;

select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2;

-- 4. remove any colunm

alter table layoffs_staging2
drop column row_num;