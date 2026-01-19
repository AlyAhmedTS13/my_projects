-- EDA 

-- fraud rate in data 
select count(*) as count_trans, count(*) *100/(select count(*) from paysim_transactions where `type` in ('TRANSFER', 'cash_out')) as percentage
from paysim_transactions
where isFraud = 1;


-- How many transactions are fraudulent vs non-fraudulent?

select isFraud,count(*) as count
from paysim_transactions
group by isFraud;

-- How many transactions are of each

select `type`,count(*) as count, 
count(*) *100/(select count(*) from paysim_transactions) as percentage
from paysim_transactions
group by `type`
order by count desc ;

-- Min, max, average and total amount
create view amount_distribution as 
select sum(amount) as total_amount ,
max(amount) as max_amount,
min(amount) as min_amount,
avg(amount) as avg_amount
from paysim_transactions;

-- Fraud count by transaction type

select `type`, count(*) as count_fraud
from paysim_transactions
where isFraud = 1
group by `type`
order by count_fraud desc ;

-- Fraud rate by type 
select `type`, round(sum(isFraud)*100/count(*),2) as percentage_fraud,
count(*) as total_trns
from paysim_transactions
group by `type`
order by percentage_fraud desc ;

-- How many transactions have balance mismatches
select count(*) as count
from paysim_transactions
where balance_mismatch_flag = 1 ;

-- Count transactions that drain account to zero
select count(*) as count
from paysim_transactions
where drained_account_flag = 1 ;

-- Fraud rate in mismatch transactions and Fraud rate in drain transactions vs normal
create view Fraud_rate_in_drain_transactions as
select drained_account_flag
,round(sum(isFraud)*100/count(*),2) as percentage_fraud,
count(*) as count
from paysim_transactions
group by drained_account_flag
order by percentage_fraud desc ;

create view Fraud_rate_in_mismatch_transactions as
select balance_mismatch_flag
,round(sum(isFraud)*100/count(*),2) as percentage_fraud,
count(*) as count
from paysim_transactions
group by balance_mismatch_flag
order by percentage_fraud desc ;

-- Average transaction amount for fraud vs non-fraud transactions
select isFraud , round(avg(amount),1)  as avg_amount
from paysim_transactions
group by isFraud
order by avg_amount desc;

-- Accounts with multiple transactions
create view Accounts_multiple_transactions as
select nameOrig, count(*) as count_trans  , sum(amount) as total_amount,
sum(isFraud) as Fraud_count
from paysim_transactions
where `type` in ('TRANSFER', 'cash_out')
group by nameOrig
order by count_trans desc
limit 20;

-- Destination accounts receiving from multiple unique senders
create view Destination_Accounts_multiple_unique_senders as
select nameDest, count(*) count_trans ,count(distinct nameOrig) unique_senders ,
sum(amount) as total_amount,sum(isFraud) as Fraud_count
from paysim_transactions
where `type` in ('TRANSFER', 'cash_out')
group by nameDest
order by unique_senders  desc
limit 20;

-- Fraud rate by hour - find peak fraud hours

select hour_of_day,count(*) as total_trans ,
sum(isFraud) as Fraud_count ,round(sum(isFraud)*100/count(*),2) as percentage_Fraud
from paysim_transactions
group by hour_of_day
order by percentage_Fraud desc ;

-- how many transactions trigger multiple risk signals and are these mostly frauds?
create view flags_fraud_ratio as 
select sum_of_flags_triggerd , count(*) as total_trans,
sum(isFraud) as Fraud_Count ,
sum(isFraud)*100/count(*) as Fraud_percentage
from
(select * ,(
balance_mismatch_flag+
overdraft_flag+
drained_account_flag
+dest_balance_mismatch_flag
+high_amount_ratio_flag
+night_transaction_flag) as sum_of_flags_triggerd
from paysim_transactions
) as t1
group by sum_of_flags_triggerd
order by Fraud_Count desc;

create view transaction_features_view as 
select amount,
oldbalanceOrg,
newbalanceOrig,
oldbalanceDest,
newbalanceDest,
balance_mismatch_flag,
overdraft_flag,
drained_account_flag,
dest_balance_mismatch_flag,
high_amount_ratio_flag,
night_transaction_flag,
hour_of_day
from paysim_transactions