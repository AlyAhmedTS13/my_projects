select *
from nashville_housing;


-- Stnadardize salesDate format 

select saledate
from nashville_housing;

alter table nashville_housing
add column saleDate_new date;

update nashville_housing
set saleDate_new = str_to_date(saledate,'%M %e, %Y');

select saledate_new
from nashville_housing;

alter table nashville_housing
drop column SaleDate;

select *
from nashville_housing;

-- populate the property address column 

select propertyaddress
from nashville_housing
where propertyaddress is null;

select t1. ParcelID ,t1.PropertyAddress,t2.ParcelID,t2.PropertyAddress
from nashville_housing as t1
join nashville_housing as t2
	on t1. ParcelID = t2.ParcelID
    and t1. UniqueID != t2.UniqueID
where t1.propertyaddress is null;


update nashville_housing as t1
join nashville_housing as t2
	on t1. ParcelID = t2.ParcelID
    and t1. UniqueID != t2.UniqueID
set t1.PropertyAddress=t2.PropertyAddress
where t1.propertyaddress is null;

-- breaking address into  individual columns ( address, city, state )

select PropertyAddress
from nashville_housing;

select PropertyAddress, substring_index(PropertyAddress,',',1) as address,
substring_index(PropertyAddress,',',-1) as city
from nashville_housing;

alter table nashville_housing
add column Property_address varchar(55);

alter table nashville_housing
add column Property_city varchar(55);


update nashville_housing
set Property_address=substring_index(PropertyAddress,',',1);

update nashville_housing
set Property_city=substring_index(PropertyAddress,',',-1);

select PropertyAddress,Property_city,Property_address
from nashville_housing;

select owneraddress ,substring_index(owneraddress,',',1),
substring_index(owneraddress,',',-1),
substring_index(substring_index(owneraddress,',',2),',',-1)
from nashville_housing;

alter table nashville_housing
add column owner_city varchar(55);

alter table nashville_housing
add column owner_address varchar(55);

alter table nashville_housing
add column owner_State varchar(55);

update nashville_housing
set owner_address =substring_index(owneraddress,',',1);

update nashville_housing
set owner_city =substring_index(substring_index(owneraddress,',',2),',',-1);

update nashville_housing
set owner_State =substring_index(owneraddress,',',-1);

select owner_city,owner_address,owner_State,OwnerAddress
from nashville_housing;


-- change N and Y to yes and no

select distinct SoldAsVacant
from nashville_housing;

select SoldAsVacant,
case
when SoldAsVacant = 'N' then 'No'
when SoldAsVacant = 'Y' then 'Yes'
else SoldAsVacant
end as SoldAsVacant_new
from nashville_housing;


update nashville_housing
 set SoldAsVacant = case
when SoldAsVacant = 'N' then 'No'
when SoldAsVacant = 'Y' then 'Yes'
else SoldAsVacant
end;

select distinct SoldAsVacant
from nashville_housing;

-- Removing duplicates
create view v1 as 
select*, row_number() over(
partition by 
	parcelid, 
    propertyaddress,
    SalePrice,
    saleDate_new,
    LegalReference
order by 
	UniqueID
    ) as row_num
from nashville_housing;

select *
from v1
where row_num >=2;

create table nashville_housing_clean as 
select*, row_number() over(
partition by 
	parcelid, 
    propertyaddress,
    SalePrice,
    saleDate_new,
    LegalReference
order by 
	UniqueID
    ) as row_num
from nashville_housing;

select *
from nashville_housing_clean;

delete 
from nashville_housing_clean
where row_num >=2;

select *
from nashville_housing_clean
where row_num >=2;

-- Remove not used columns 

ALTER TABLE nashville_housing_clean
DROP COLUMN address,
DROP COLUMN taxdistrict,
DROP COLUMN propertyaddress,
DROP COLUMN owneraddress,
DROP COLUMN  row_num;


