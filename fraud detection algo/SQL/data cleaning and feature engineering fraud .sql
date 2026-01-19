select count(*)
from paysim_transactions;

-- cheack impossible numeric values 

select count(*) as count
from paysim_transactions
where amount <= 0 ;

select count(*) as count
from paysim_transactions
where oldbalanceOrg < 0 ;

select count(*) as count
from paysim_transactions
where oldbalanceDest < 0 ;

-- remove 16 rows where amount of transaction was 0 or a negative value which cant happen 
delete
from paysim_transactions
where amount <= 0 ;

-- check for null values  

select count(*)
from paysim_transactions
where `type` is null ;


select count(*)
from paysim_transactions
where `step` is null ;


select count(*)
from paysim_transactions
where `nameOrig` is null ;

select count(*)
from paysim_transactions
where `nameDest` is null ;

-- our data is clean it has no null values and no impossible  numeric values 
-- now we will start with making or new signal columns to use latter for our ml fraud detection algo

-- first we will check most type of trnasctions where fraud happens 
select `type` , count(*)
from paysim_transactions
where isFraud = 1
group by `type`;

-- fraud only happens in trnasfer and cash out we will take that in consideration 


select count(*) as count, case  
when oldbalanceOrg-amount != newbalanceOrig and `type` in ('TRANSFER', 'cash_out') then 1
else 0 end as balance_mismatch_flag
from paysim_transactions
group by balance_mismatch_flag;




-- making blacne mismatch flag column 

alter table paysim_transactions
add column balance_mismatch_flag tinyint ;

update paysim_transactions
set balance_mismatch_flag = case  
when oldbalanceOrg-amount != newbalanceOrig and `type` in ('TRANSFER', 'cash_out') then 1
else 0 end ;

select balance_mismatch_flag, count(*) as count
from paysim_transactions
group by balance_mismatch_flag;

-- over draft flag ( Aggressive spending )

select count(*), case 
when amount > oldbalanceOrg  and `type` in ('TRANSFER', 'cash_out') then 1
else 0  end as overdraft_flag
from paysim_transactions
group by overdraft_flag;

alter table paysim_transactions
add column overdraft_flag tinyint;

update
paysim_transactions
set overdraft_flag = case 
when amount > oldbalanceOrg  and `type` in ('TRANSFER', 'cash_out') then 1
else 0  end;

select overdraft_flag, count(*) as count
from paysim_transactions
group by overdraft_flag;


-- drained account flag column ( Account emptied after transaction )


select count(*), case
when newbalanceOrig = 0 and `type` in ('TRANSFER', 'cash_out') then 1
else 0 end as drained_account_flag
from paysim_transactions
group by drained_account_flag;


alter table paysim_transactions
add column drained_account_flag tinyint;

update
paysim_transactions
set drained_account_flag = case 
when newbalanceOrig = 0  and `type` in ('TRANSFER', 'cash_out') then 1
else 0  end;


select drained_account_flag, count(*) as count
from paysim_transactions
group by drained_account_flag;


-- destnation balance mismatch flag column

select count(*), case
when oldbalanceDest + amount != newbalanceDest and `type` in ('TRANSFER', 'cash_out') then 1
else 0 end as dest_balance_mismatch_flag
from paysim_transactions
group by dest_balance_mismatch_flag;


alter table paysim_transactions
add column dest_balance_mismatch_flag tinyint;


update paysim_transactions 
set dest_balance_mismatch_flag = case
when oldbalanceDest + amount != newbalanceDest and `type` in ('TRANSFER', 'cash_out') then 1
else 0 end;

select dest_balance_mismatch_flag, count(*) as count
from paysim_transactions
group by dest_balance_mismatch_flag;

-- Large transaction relative to balance flag ( more than or equal 90% of balance)

select count(*), case
when amount >= 0.9*oldbalanceOrg and `type` in ('TRANSFER', 'cash_out') then 1
else 0 end as high_amount_ratio_flag
from paysim_transactions
group by high_amount_ratio_flag;

alter table paysim_transactions
add column high_amount_ratio_flag tinyint;


update paysim_transactions
set high_amount_ratio_flag = case
when amount >= 0.9*oldbalanceOrg and `type` in ('TRANSFER', 'cash_out') then 1
else 0 end;

select high_amount_ratio_flag, count(*) as count
from paysim_transactions
group by high_amount_ratio_flag;


-- making hour of day column using step column

select step%24 as hour_of_day, count(*)
from paysim_transactions
group by hour_of_day;


alter table paysim_transactions
add column hour_of_day tinyint;

update paysim_transactions
set hour_of_day = step%24 ;

-- trnascation happend at night column ( classic metric as most fraud still happen late when most users are asleep)

select count(*), case
when hour_of_day in (0,1,2,3,4,5,6) and `type` in ('TRANSFER', 'cash_out') then 1
else 0 end as night_transaction_flag
from paysim_transactions
group by night_transaction_flag;

alter table paysim_transactions
add column night_transaction_flag tinyint;

update paysim_transactions
set night_transaction_flag = case
when hour_of_day in (0,1,2,3,4,5,6) and `type` in ('TRANSFER', 'cash_out') then 1
else 0 end;

select night_transaction_flag , count(*)
from paysim_transactions
group by night_transaction_flag;



-- last thing we will do is adding index for columns we will use a lot so our EDA and query time is faster

-- Index on sender account
CREATE INDEX idx_nameOrig ON paysim_transactions(nameOrig);

-- Index on receiver account
CREATE INDEX idx_nameDest ON paysim_transactions(nameDest);

-- Index on transaction type
CREATE INDEX idx_type ON paysim_transactions(`type`);

-- Index on hour of day
CREATE INDEX idx_hour_of_day ON paysim_transactions(hour_of_day);
