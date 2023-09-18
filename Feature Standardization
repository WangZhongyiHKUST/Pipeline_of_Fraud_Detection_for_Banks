# Standardization of transaction-level feature engineering in bank trades

-  In the domain of fraud detection(private banking affairs), some features are tested fruitful and can be standardized for repeated use. Hence, a standardization document for extracting useful features interacting with database is written. And with minimal modification with respect to different databases, those features can be obtained and used for later modeling directly.

## 1. Pre-defined and processed columns(in a database table)
Suppose we have obtained a wide table containing information needed after data preprocessing.

+ (1). Static/basic Information
```sql
cust_name           --customer's name    
cust_gender         --customer's gender
cust_nation         --customer's nationality
cust_birth_date     --customer's birth date
cust_occupation     --customer's occupation
cust_cert_type      --customer's certificate type
cust_cert_num       --customer's certificate No.
cust_phone_tel      --customer's phone
acct_no             --customer's account
sub_acct_no         --customer's sub account
card_no             --customer's card No.
cust_id             --customer's unique id
```
  
+ (2). Transactional Information
```sql
open_acct_dt        --card openning date
close_acct_dt       --card closed date
trans_dt            --trade date
trans_dt_unix       --trade date in unixtime format
trans_time          --trade time with format of HH:mm:ss
trans_time_unix     --trade time in unixtime format
trans_amt           --trade amount of money
trans_amt_str       --trade amount of money in string
deb_cre_flag        --in or out flag, C for in, D for out
balance             --remaining money in the card
transfer_acct       --opponent's account no
transfer_name       --opponent's account name
transfer_bank_no    --opponent's bank No.
transfer_bank       --opponent's bank name
remarks_code        --remark code
remarks             --remarks in Chinese language
postscript          --postscript in Chinese language
dt                  --partition column date, yyyy-MM-dd)
```
  

default settings: typically, most static information is of string type, date information is of timestamp or 'yyyyMMdd' or 'yyyy-MM-dd' format, while money is of float type.

## 2.Standard feature extraction from the wide table above

Suppose a table named test.base is created and features are inserted into this table in the database, we can further extract infos.
SQL language is used when writing the process.

```sql
DROP TABLE IF EXISTS test.base_transfeature;
CREATE TABLE test.base_transfeature AS
select a.*
,case when substr(trans_time,1,2) in ('23', '00', '01', '02', '03', '04', '05', '06') \
then 1 else 0 end as ind_night_trans    --whether traded at late night
,case when substr(trans_time,1,2) in ('23', '00', '01') \
then 1 else 0 end as ind_night_23_1_trans  --whether traded during 23pm-1am

,case when deb_cre_flag = 'C' then 1 else 0 end as ind_trans_in     --whether trading in
,case when deb_cre_flag = 'D' then 1 else 0 end as ind_trans_out    --whether trading out

--blanks are specialized according to different situations in order to make the judgement
,case when                    then 1 else 0 end as ind_atm --whether draw from atm machine
,case when                    then 1 else 0 end as ind_third_party --whether from third party
,case when                    then 1 else 0 end as ind_cross_bank --whether trading across banks
,case when                    then 1 else 0 end as ind_counter_service --whether from bank counter
,case when                    then 1 else 0 end as ind_not_common_third_party --whether from uncommon third parties
,case when                    then 1 else 0 end as ind_salary --whether payroll service

--related to money amount, those ranges can be modified accordingly
,case when trans_amt between 48000 and 50000 then 1 else 0 end as ind_trans_amt_48k_50k  --whether between 48k and 50k
,case when trans_amt <= 200 and trans_amt > 0 then 1 else 0 end as ind_trans_amt_lt200 --whether no greater than 200
,case when trans_amt <= 10 and trans_amt > 0 then 1 else 0 end as ind_trans_amt_lt10  --whether money no greater than 10
,case when trans_amt <= 0 then 1 else 0 end as ind_trans_amt_l0 --whether money less then  0
,case when trans_amt >= 1000 then 1 else 0 end as ind_trans_amt_gt1k  --whether money no less than 1k
,case when trans_amt >= 4000 then 1 else 0 end as ind_trans_amt_gt4k  --whether money no less than 4k
,case when balance <= 10 then 1 else 0 end as ind_trans_bal_lt10  --whether balance no greater than 10
,case when balance <= 500 then 1 else 0 end as ind_trans_bal_l500 --whether balance no greater than 500

--postscript related
,case when postscript regexp '基金|充值|取现' then 1 else 0 end as ind_trans_fund  --whether contains expressions such as '基金|充值|取现' 
,case when postscript regexp '博彩|时时彩|赌博|赌波|dubo|aomen|bcj|会员账号[0-9]+|ssc|会员号|加微' then 1 else 0 end as ind_spe_post1 --whether contains special expressions
,case when postscript NOT regexp '店|公安局' then 1 else 0 end as ind_spe_post2 --get rid of '店|公安局' 
,case when postscript regexp '^[0-9]{4,6}$' then 1 else 0 end as ind_spe_post3 --4th-6th digits of a number
,case when postscript regexp '^[0-9]+[a-zA-Z]+$' then 1 else 0 end as ind_spe_post4 --whether contains numbers and digits
,case when postscript regexp '会员' and replace(trim(postscript), '会员', '') regexp '^[0-9]+[a-zA-Z]+$' then 1 else 0 end as ind_spe_post5 --whether contains '会员'+digits+characters
,case when postscript regexp '充值' and replace(trim(postscript), '充值', '') regexp '^[0-9]+[a-zA-Z]+$' then 1 else 0 end as ind_spe_post6 --whether contains '充值'+digits+characters
,case when postscript regexp '报警|还给我|钱退回来|钱退给我' then 1 else 0 end as ind_spe_post7

--some special number of money
,case when 
	(CAST(substr(trans_amt_str, length(trans_amt_str)-1,2) AS INTEGER)%11 = 0  AND CAST(substr(trans_amt_str, length(trans_amt_str)-1,2) AS INTEGER)>0)
	or substr(trans_amt_str, length(trans_amt_str)-1,2) > '91' then 1 else 0 END as ind_trans_spe_amt  --special trade money，an integer or special decimals

,case when ((trans_amt/(round(trans_amt/1000)*1000)>=0.95 and trans_amt/(round(trans_amt/1000)*1000)<=1.05) AND trans_amt>=1000)
    OR ((trans_amt/(round(trans_amt/100)*100)>=0.95 and trans_amt/(round(trans_amt/100)*100)<=1.05)  AND trans_amt<1000 AND trans_amt>0)  
	then 1 else 0 end as ind_trans_close_int --whether close to an integer

,case when trim(cust_name) = transfer_name then 1 else 0 end as ind_trans_self  --whether trade in person
,case when length(transfer_name) <=3 then 1 else 0 end as ind_trans_person  --whether trade privately(currently judged by length of names)
,case when length(transfer_name) >3 then 1 else 0 end as ind_trans_company  --whether trade in inprivate

,lag(trans_amt,1) over(partition by acct_no order by trans_dt,trans_time) as last_trans_amt --last trade money
,lag(trans_time_unix,1) over(partition by acct_no order by trans_dt,trans_time) as last_trans_time_unix--last trade time
,lag(deb_cre_flag,1) over(partition by acct_no order by trans_dt,trans_time) as last_trans_in_out  --last trade out
,lead(deb_cre_flag,1) over(partition by acct_no order by trans_dt,trans_time) as next_trans_in_out --next trade out

,sum(case when deb_cre_flag = 'C' then trans_amt else 0 end) over(partition by acct_no order by trans_dt,trans_time) as cum_sum_trans_amt_in --cumulative in
,sum(case when deb_cre_flag = 'D' then trans_amt else 0 end) over(partition by acct_no order by trans_dt,trans_time) as cum_sum_trans_amt_out --cumulative out
from test.base
where dt between DateA and DateB
```
## 3. Aggregate features in a daily manner(day-dimension)
```sql
DROP TABLE IF EXISTS test.base_transfeatureByDt;
CREATE TABLE test.base_transfeatureByDt AS
select
acct_no
,sub_acct_no
,card_no
,cust_id
,trans_dt
,trans_dt_unix

,sum(trans_amt) as sum_trans_amt --daily trade money
,count(1) as count_trans --daily trade counts
,sum(ind_trans_out*trans_amt) as sum_trans_amt_out --daily trade out
,sum(ind_trans_out) as count_trans_out --daily out counts
,sum(ind_trans_in*trans_amt) as sum_trans_amt_in  --daily in money
,sum(ind_trans_in) as count_trans_in --daily in counts
,case when sum(ind_trans_out*trans_amt) = 0 then null else  sum(ind_trans_in*trans_amt)/sum(ind_trans_out*trans_amt) end as ratio_trans_amt_in_out --daily money in/out ratio

,max(trans_amt) as max_trans_amt --daily max trade money
,min(trans_amt) as min_trans_amt --daily min trade money

,sum(ind_atm*trans_amt) as  sum_trans_amt_atm --daily atm cash out
,sum(ind_atm) as count_trans_atm --daily atm cash counts
,sum(ind_trans_amt_48k_50k) as count_trans_48k_50k --daily trade money between 48k and 50k counts

,sum(ind_night_trans*trans_amt) as sum_trans_amt_night -- daily 0-6am trade money
,sum(ind_night_trans) as count_trans_night -- daily 0-6am trade counts
,sum(ind_trans_out*ind_night_trans) as count_trans_out_night -- daily 0-6am out counts

,sum(ind_atm*ind_night_23_1_trans) as count_trans_night_atm -- daily 23pm-1am atm cash counts

,sum(ind_third_party*trans_amt) as sum_trans_amt_third_party --daily trade with third party amount
,sum(ind_third_party) as count_trans_third_party --daily trade with third party counts

,sum(ind_third_party*trans_amt*ind_trans_out) as sum_trans_amt_third_party_out --daily trade with third party out money
,sum(ind_third_party*ind_trans_out) as count_trans_third_party_out --daily trade with third party out counts

,sum(ind_third_party*trans_amt*ind_trans_in) as sum_trans_amt_third_party_in --daily third party trade in money
,sum(ind_third_party*ind_trans_in) as count_trans_third_party_in --daily third party trade counts
,sum(ind_not_common_third_party) as count_trans_not_common_third_party --daily uncommon third party trade counts

,sum(ind_counter_service) as count_trans_counter_service --daily bank counter trade counts
,sum(ind_counter_service*ind_trans_out) as count_trans_counter_service_out --daily bank counter out trade counts

,sum(ind_cross_bank) as count_trans_cross_bank --daily interbank trade counts
,sum(ind_cross_bank*ind_trans_out) as count_trans_cross_bank_out --daily interbank out trade counts
,sum(ind_cross_bank*ind_trans_in) as count_trans_cross_bank_in --daily interbank in trade counts

,sum(ind_trans_amt_lt200) as count_trans_amt_lt200 --daily number of trades with amount < 200
,sum(ind_trans_amt_lt10) as count_trans_amt_lt10 --daily number of trades with amount < 10
,sum(ind_trans_amt_lt0) as count_trans_amt_lt0  --daily number of trades with amount < 0
,sum(ind_trans_amt_gt1k) as count_trans_amt_gt1k --daily number of trades with amount > 1000
,sum(ind_trans_amt_gt4k) as count_trans_amt_gt4k --daily number of trades with amount > 4000

,sum(ind_trans_bal_lt10*ind_trans_out) as count_trans_bal_lt10 --daily number of trade out with balance < 10 
,sum(ind_trans_bal_l500*ind_trans_out) as count_trans_bal_l500--daily number of trade out with balance < 500

,sum(trans_amt*ind_spe_post7) as sum_trans_amt_spe_remarks1 --daily trade amounts of remarks containing information reporting to police 
,sum(ind_spe_post7) as count_trans_spe_remarks1 --daily number of trades with remarks containing report information

,sum(ind_trans_in*ind_spe_post2*greatest(ind_spe_post1,ind_spe_post3,ind_spe_post4,ind_spe_post5,ind_spe_post6)) as count_trans_in_spe_remarks2 --daily nuber of trades with special information in remarks/postscript

,sum(ind_trans_spe_amt*ind_trans_out) as count_trans_out_spe_amt --daily number of trade out with speical amount
,sum(ind_trans_spe_amt*ind_trans_in) as count_trans_in_spe_amt --daily number of trade in with special amount

,sum(ind_trans_person*ind_trans_out*trans_amt) as sum_trans_amt_person_out --daily amount of trade out when opponent is a person
,sum(ind_trans_person*ind_trans_out) as count_trans_person_out --daily number of trade out when opponent is a person
,sum(ind_trans_person*ind_trans_in*trans_amt) as sum_trans_amt_person_in --daily amount of trade in when opponent is a person
,sum(ind_trans_person*ind_trans_in) as count_trans_person_in --daily number of trade in when opponent is a person


,sum(ind_trans_company*ind_trans_out*trans_amt) as sum_trans_amt_company_out --daily amount of trade out with companies

,sum(case when last_trans_in_out != deb_cre_flag and last_trans_amt/trans_amt between 0.9 and 1.1 then 1 else 0 end) as count_last_trans_spe_ratio --daily number of trade out with a special ratio

,sum(ind_salary) as count_salary --amount of payroll trades


,count(distinct case when ind_trans_out = 1 then substr(trans_time,1,2) else null end) as count_distinct_trans_out_hour --number different hours when trade out happens


,sum(ind_trans_close_int) as count_trans_close_int --number of trades closing to an integer
,sum(ind_trans_out*ind_trans_close_int) as count_trans_out_close_int --number of trades out closing ot an integer


,sum(case when trans_time_unix - last_trans_time_unix <= 120 then 1 else 0 end) as ind_trans_interval_2m --number of trades that intervals less than 2 minutes 
,sum(case when trans_time_unix - last_trans_time_unix <= 60 then 1 else 0 end) as ind_trans_interval_1m -- number of trades that intervals less than 1 minute

,dt
from test.base_transfeature
where dt between DateA and DateB
group by 
acct_no
--,sub_acct_no
--,card_no
--,cust_id
--,trans_dt
--,trans_dt_unix
,dt;
```
## 4.Aggregate monthly features using daily features

Up to now, features can iterate weekly and monthly. In practice, some features in 1 month, 2 or 3 months are used.

example:
if we want to get the trade money in a month, 
a simple join operation of daily feature table is applied and should be aggregated by date.
```sql
select a.acct_no
,a.dt
,sum(b.sum_trans_amt) as sum_trans_amt_1_2m --trade money in a month
,sum(b.count_trans) as count_trans_1_2m --trade counts in a month

from  base_transfeatureByDt a --daily feature table left join itself
left join base_transfeatureByDt b 
on a.acct_no = b.acct_no
and to_date(a.dt,'yyyymmdd') >= DATEADD(to_date(b.dt,'yyyymmdd'),1,'month') 
and to_date(a.dt,'yyyymmdd') <= DATEADD(to_date(b.dt,'yyyymmdd'),2,'month')
;
```


