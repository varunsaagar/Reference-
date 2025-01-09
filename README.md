Find the total number of smart phone upgrade after Jun month grouped by month and exclude any orders through resellers	"select FORMAT_DATE('%b %Y', coalesce(a.invc_dt,a.pymnt_dt)) session_mth, count(*)
FROM `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.equip_sum_fact` a
WHERE a.rpt_mth > '2024-06-30'
    AND a.acq_ret_ind = 'R'
    AND a.NET_SALES>0
    AND a.FIN_UPG_FLAG='Y'
    AND upper(a.ORDER_TYPE) = 'PS'
    AND a.REV_GEN_IND='Y'
    AND a.sls_dist_chnl_type_cd <> 'W'
    AND trim(lower(coalesce(a.device_prod_nm, a.prod_nm))) IN (SELECT trim(lower(prod_nm)) FROM `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.device_dp_map` WHERE lower(data_tier_fin) = 'smartphone') 
    group by 1"
Find the number of abandoned calls based on department for the latest available information but exclude prepay calls. I also need to know average, min and max time before the call is abandoned	"SELECT    ECCR_DEPT_NM  , COUNT(ACD_AREA_NM), AVG(TIME_TO_ABAND_SECONDS), MIN(TIME_TO_ABAND_SECONDS), MAX(TIME_TO_ABAND_SECONDS)
FROM `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.icm_summary_fact` WHERE ABANDONS_CNT = 1 AND CALL_END_DT = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)  AND ACD_AREA_NM <> 'Prepay'
GROUP BY ECCR_DEPT_NM"
Find the number of first time callers on Jun 1st who did not call before in the last 30 days	"with callbase as (
SELECT * FROM `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.icm_summary_fact` where call_answer_dt = cast('2024-06-01' as date) and acss_call_id is not null  )

,last30dayscallers as (
select distinct cust_id,mtn from `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.icm_summary_fact` where call_answer_dt between cast('2024-06-01' as date) - 30 and cast('2024-06-01' as date) - 1 and acss_call_id is not null  )

--,firsttimecallers as (
  select count(*) from callbase a left join last30dayscallers b on (concat(a.cust_id,a.mtn)) = (concat(b.cust_id,b.mtn)) where b.cust_id is null
--)"
Find the number of postpaid customers using Mobile and Home products and also group by customer having only mobile, only home and both mobile and home products	"select mobile_home_grp, count(1) from (
Select cust_id,acct_num,
case 
  when line_cnt=1 and fwa_ind=1 then 'Home_Only_acct'
  when line_cnt>1 and fwa_ind=1 then 'Mobile_Home_acct'
  when line_cnt>=1 and fwa_ind=0 then 'Mobile_Only_acct'
else null
end as mobile_home_grp
from 
(
Select cust_id,acct_num ,count(distinct mtn) as line_cnt,
max(case when fwa_ind='Y' then 1 else 0 end) as fwa_ind from 
(
select cust_id,acct_num,mtn,
CASE
              WHEN Upper ( PPLAN.coe_pplan_sub_type_desc ) LIKE '%HOME%'
              THEN 'Y'
              ELSE 'N'
          END AS fwa_ind
 FROM 
  vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.subs_sum_fact SSF

  INNER JOIN vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.vz2_segmt_dim_ref SEG
    ON SEG.VZ2_SEGMT_CD = SSF.vz2_segmt_cd
    AND SEG.CURR_PREV_IND = 'C'
    AND SEG.VZ2_SEGMT_CTGRY_DESC = 'Wireless Consumer Revenue'

LEFT JOIN vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.price_plan PPLAN 
    on ssf.pplan_cd = PPLAN.pplan_cd
    AND upper(PPLAN.coe_pplan_service_type_desc) = 'POSTPAID'

WHERE 
    SSF.RPT_MTH = '2024-05-01'
    AND COALESCE(TRIM(SSF.MANAGED_IND),'C') <> 'U' -- CONSOLIDATED MARKETS ONLY
    AND COALESCE(SSF.LINE_TYPE_CD, 'X') NOT IN ('H','0','T') --  FILTER OUT M2M
    AND COALESCE(SSF.PREPAID_IND, 'N') <> 'Y'  -- FILTER OUT PREPAID  
)x
group by 1,2
)Z
group by 1,2,3)
group by 1"
How many customers have late payment fee for the current month	SELECT count(*) FROM `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.acct_bill` where cyc_mth_yr = '202411' and late_pymnt_chrg_amt > 0
Find all the plan code for all myPlans 	SELECT * FROM `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.price_plan` where coe_pplan_ctgry_desc = 'myPlans'
Find all active customers who have myPlans and limit to smartphone plans only 	"SELECT pplan_cd, count(*) FROM `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.cust_acct_line_pplan` a left join `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.cust_acct_line` b on (a.cust_id = b.cust_id and a.cust_line_seq_id = b.cust_line_seq_id)
where
b.mtn_status_ind = 'A' and 
b.vsn_cust_type_cd in ('PE','ME') and
a.pplan_cd in ('63217','63215','69185')
group by a.pplan_cd;

"
Find me total number of IVR calls for 2024 and categorize by month	"SELECT EXTRACT(MONTH FROM ivr_call_dt), count(*) FROM `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.ivr_call` where ivr_Call_id is not null 
group by EXTRACT(MONTH FROM ivr_call_dt) << 2024 filter needed >>"
Please find the total number of calls received by agents for current <any> month	SELECT count(*) FROM `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.icm_summary_fact` where EXTRACT(MONTH FROM call_answer_dt) = 11 and  ACD_AREA_NM <> 'Prepay' LIMIT 1000
Find the number of customers who called again in 3 days or more	"SELECT * FROM `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.acss_repeat_call_detail` 
where repeat_same_day = 0 and repeat_3_day > 0 and call_start_dt > current_date() - 30"
Find all Fixed 5G consumer customers who disconnect in the last 30 days with service period less 6 months	"SELECT
    cust_id,
    ordernumber,
    location_code,
    mtn,
    sales_dt,
    disconnect_dt,
    -- vz2_segmt_ctgry_desc,
    -- cancel_level_3_desc,
    -- cancel_level_4_desc,
    DATETIME_DIFF(disconnect_dt,sales_dt,DAY) AS days_To_Disconnect,
  FROM vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.fixed_5g_summary_fact`
  WHERE segment = 'CONSUMER'
  AND status = 'DISCONNECT'
  AND disconnect_dt >= '2024-11-01'"
find all the FWA customers who bought the service more than 3 days ago without pro install and still not activated it	"SELECT DISTINCT sales_dt,ship_dt, cust_id,mtn,ordernumber, location_code, status, install_type
                FROM vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.fixed_5g_summary_fact
                WHERE 0 = 0
                AND segment = 'CONSUMER'
                AND sales_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
                AND ship_dt IS NOT NULL
                AND  (status != 'CANCEL' OR cancel_dt IS NULL)
                AND ispu_status IS NULL -- excluding ISPU and ISP
                AND (ntls_order_status_desc IN ('ACTIVATED', 'PLACED') OR ntls_order_status_desc IS NULL) -- exclude cancels and deactivated
                AND install_type != 'PRO INSTALL'"
Find the overall call handle time by Call Type and separately for Internal and SPC for given date	"SELECT
    isf.CALL_ANSWER_DT CALL_START_DT,ISF.ECCR_SUPER_LINE_BUS_NM AS CALL_TYPE,
        --FWA_IND,
        CASE WHEN isf.ACD_AREA_NM ='SPC' THEN 'SPC'
    WHEN isf.ACD_AREA_NM IS NULL THEN NULL
    ELSE 'Internal' end as Area_nm,
/*  CASE WHEN isf.ECCR_DEPT_NM IN ('Care','Other','LNP') THEN 'Care' 
        WHEN isf.ECCR_DEPT_NM IN ('Tech','Tier 1 Tech') THEN 'Tech'
        ELSE isf.ECCR_DEPT_NM END CALL_TYPE,*/
    Sum(isf.ANSWERED_CNT) CALLS_ANSWERED,
    Sum(Cast(isf.HANDLE_TM_SECONDS AS BIGINT)) HANDLE_TM
FROM vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.icm_summary_fact isf
WHERE 
isf.ECCR_DEPT_NM IN ('Care','Tech','Bilingual','LNP','Other','Tier 1 Tech')
AND
isf.CALL_ANSWER_DT  = '2024-11-22' 
GROUP BY 1,2,3"
Find the total number of calls transferred in the last 30 days by day wise	"select call_answer_dt, count(ivr_call_id) from (
select        
                        call_answer_dt,
                        ivr_call_id,count(distinct acss_call_id)  as call_transfer_count ,
                        sum(handle_tm_seconds) as handle_tm_seconds,
                        sum(call_duration_seconds) as call_duration_seconds 
                from         
                        vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.icm_summary_fact
                where 
                        --call_answer_dt between date_sub(CAST('2024-10-06' as DATE), interval 15 DAY) and CAST('2024-10-06' as DATE)
                        call_answer_dt between '2024-10-24' and '2024-11-28'
          
                        and acss_call_id is not null
                group by call_answer_dt,ivr_call_id)
    where call_transfer_count > 1
    group by call_answer_dt"
Find the total of prepaid and postpaid calls to IVR in the last 30 days	SELECT prepaid_ind, count(*) FROM `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.ivr_call` where ivr_call_dt >= current_date() - 30 group by prepaid_ind LIMIT 1000
Find number of bills generated for each day in the month of october	SELECT EXTRACT(DAY FROM bill_dt), count(*) FROM `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.acct_bill` where cyc_mth_yr = '202410' group by EXTRACT(DAY FROM bill_dt)
Find the sum of all late fee reversal in the month of October	"
SELECT sum(late_pymnt_chrg_amt) FROM `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.acct_bill` where late_pymnt_chrg_amt < 0 and cyc_mth_yr = '202410' "
"How many trade-in orders for the month of October
"	SELECT count(*) FROM `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.equip_sum_fact` where tradein_ind = 'Y' and duplicate_ind = 'N' and rpt_mth = '2024-10-01' and area_cd = 'MW'
How many active lines have devices older than 24 months	"SELECT count(*) FROM `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.device_usage_age` a
left join `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.cust_acct_line` b on (a.cust_id = b.cust_id and a.cust_line_seq_id = b.cust_line_seq_id)
where 
b.mtn_status_ind = 'A'
and b.vsn_cust_type_cd in ('PE', 'ME') and
usage_months > 24 and
rpt_mth = '2024-10-01'"
Find total number of new customers in the first week of November	"
Select 
distinct 
                        ESF.CUST_ID,ESF.MTN,ESF.ACCT_NUM,
                        esf.invc_num,
      COALESCE(ESF.INVC_DT,ESF.PYMNT_DT) as Sale_DT ,        
FROM
vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.equip_sum_fact ESF        
LEFT JOIN
                        vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.cust_acct CA ON
                        ESF.CUST_ID = CA.CUST_ID AND
                        ESF.ACCT_NUM = CA.ACCT_NUM                        
WHERE 
upper(FIN_TOT_FLAG)='Y' 
AND upper(ORDER_TYPE) = 'PS' 
AND upper(ACQ_RET_IND) = 'A'
AND COALESCE(INVC_DT, PYMNT_DT) BETWEEN ACCT_ESTB_DT AND ACCT_ESTB_DT + 60 
AND upper(ESF.SOR_ID) ='V'
AND upper(ESF.ROW_TYPE_ID)<>'PR'
and TRADEIN_IND is null
AND COALESCE(ESF.INVC_DT,ESF.PYMNT_DT) BETWEEN '2024-11-01' AND '2024-11-07'"
Find the surge caller (customer who have called 3 time or more within 7 days) for last 3 months	"select cust_id
,mtn 
,cust_line_seq_id
,'ACSS' as channel_cd
,date(call_answer_dt)
,case when call_num > 3 and repeat_calls between 0 and 7 then 'Y'  end as surge_caller
from (
select DATE_TRUNC(date( a.call_answer_dt), month) as call_mth
,a.mtn
,a.cust_id 
,a.cust_line_seq_id
,a.acss_call_id
,a.call_answer_dt
,a.call_answer_tm
,ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC(date( a.call_answer_dt), month),a.cust_id,a.mtn order by DATE_TRUNC(date( a.call_answer_dt), month),a.cust_id,a.mtn,a.call_answer_dt,a.call_answer_tm asc ) AS call_num 
,date_diff(cast(max(a.call_answer_dt) over( partition by DATE_TRUNC(date( a.call_answer_dt), month),a.cust_id,a.mtn order by DATE_TRUNC(date( a.call_answer_dt), month),a.cust_id,a.mtn,a.call_answer_dt,a.call_answer_tm asc) as date)
,cast( min(a.call_answer_dt) over( partition by DATE_TRUNC(date( a.call_answer_dt), month),a.cust_id,a.mtn order by DATE_TRUNC(date( a.call_answer_dt), month),a.cust_id,a.mtn,a.call_answer_dt,a.call_answer_tm asc) as date), DAY) as repeat_calls
   from `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.icm_summary_fact` a 
   where date(a.call_answer_dt) between  '2024-07-01' and '2024-10-31'
   and a.cust_id is not null
)"
Find all customers who were put on hold for more than time 5 mins in last month	"select a.cust_id
,a.mtn 
,a.cust_line_seq_id
,date(call_answer_dt) as signal_dt
,ivr_call_id, cust_value, high_risk_ind ,transfer_point, eccr_line_bus_nm,ECCR_DEPT_NM,ecc_sm_ind,answered_cnt,acss_call_id,talk_tm_seconds,hold_tm_seconds,hold_tm_seconds,transfer_flag,call_answer_tm,call_end_tm
from vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.icm_summary_fact a
where 
  date(call_answer_dt) between '2023-10-01' and '2023-10-02'
  and answered_cnt = 1 and hold_tm_seconds > 300"
Find all customers with overall talk time greater than 5 mins for last month	"select a.cust_id
,a.mtn 
,a.cust_line_seq_id
,date(call_answer_dt) as signal_dt
,ivr_call_id, cust_value, high_risk_ind ,transfer_point, eccr_line_bus_nm,ECCR_DEPT_NM,ecc_sm_ind,answered_cnt,acss_call_id,talk_tm_seconds,handle_tm_seconds,hold_tm_seconds,transfer_flag,call_answer_tm,call_end_tm
from vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.icm_summary_fact a
where 
  date(call_answer_dt) between '2023-10-01' and '2023-10-31'
  and answered_cnt = 1 and handle_tm_seconds > 300"
How many customers moved from lower decile to high decile from last month to current month	"With lastmonthlowdecile as (
select cust_id,cust_line_seq_id from  `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.churn_sum_fact` where
score_decile_in_mkt in (1,2,3,4)
and mth = '2024-10-01')

,currentmonthhighdecile as (
 select cust_id,cust_line_seq_id from  `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.churn_sum_fact` where
score_decile_in_mkt in (8,9,10)
and mth = '2024-11-01' 
)

select count(*) from currentmonthhighdecile a left join lastmonthlowdecile b on (a.cust_id = b.cust_id and a.cust_line_seq_id = b.cust_line_seq_id)
"
How many of our unlimited ulitmate customers are in higher churn and has issues with our price for october month	"select * from  `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.churn_sum_fact` where
coe_pplan_sub_type_desc = 'Unlimited Ultimate'
and score_decile_in_mkt = 10
and primary_driver = 'pricing'
and mth = '2024-10-01' "
How many of our unlimited ulitmate customers are in higher churn for october month	"select * from  `vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.churn_sum_fact` where
coe_pplan_sub_type_desc = 'Unlimited Ultimate'
and score_decile_in_mkt = 10
and mth = '2024-10-01' "
Find total number upgrades in the first week of November	"Select 
distinct 
                        ESF.CUST_ID,ESF.MTN,ESF.ACCT_NUM,
                        esf.invc_num,
      COALESCE(ESF.INVC_DT,ESF.PYMNT_DT) as Sale_DT ,        
FROM
vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.equip_sum_fact ESF                            
WHERE 
    acq_ret_ind = 'R'
    AND NET_SALES>0
    AND FIN_UPG_FLAG='Y'
    AND upper(ORDER_TYPE) = 'PS'
    AND REV_GEN_IND='Y'
AND COALESCE(ESF.INVC_DT,ESF.PYMNT_DT) BETWEEN '2024-11-01' AND '2024-11-07'"
Find total number AAL in the month of November	"Select 
distinct 
                        ESF.CUST_ID,ESF.MTN,ESF.ACCT_NUM,
                        esf.invc_num,
      COALESCE(ESF.INVC_DT,ESF.PYMNT_DT) as Sale_DT ,        
FROM
vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.equip_sum_fact ESF        
                 WHERE 
upper(FIN_TOT_FLAG)='Y' 
AND upper(ORDER_TYPE) = 'PS' 
AND upper(ACQ_RET_IND) = 'A'
AND COALESCE(INVC_DT, PYMNT_DT) NOT BETWEEN ACCT_ESTB_DT AND ACCT_ESTB_DT + 60 
AND upper(ESF.SOR_ID) ='V'
AND upper(ESF.ROW_TYPE_ID)<>'PR'
and TRADEIN_IND is null
AND COALESCE(ESF.INVC_DT,ESF.PYMNT_DT) BETWEEN '2024-11-01' AND '2024-11-30'"
Find last month trend of Upgrades	"select sale_month, count(*) from (
Select 
distinct 
                        ESF.CUST_ID,ESF.MTN,ESF.ACCT_NUM,
                        esf.invc_num,
      Extract(month from (COALESCE(ESF.INVC_DT,ESF.PYMNT_DT))) as Sale_Month ,        
FROM
vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.equip_sum_fact ESF                             
WHERE 
    acq_ret_ind = 'R'
    AND NET_SALES>0
    AND FIN_UPG_FLAG='Y'
    AND upper(ORDER_TYPE) = 'PS'
    AND REV_GEN_IND='Y'
AND COALESCE(ESF.INVC_DT,ESF.PYMNT_DT) BETWEEN '2024-07-01' AND '2024-09-30')
group by sale_month"
Find the voluntary disconnects for the month of september	"SELECT 
cust_id,acct_num,cust_line_seq_id,mtn,
channel_type as channel_cd
FROM vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.dla_sum_fact d
JOIN vz-it-pr-gk1v-cwlsdo-0.vzw_uda_prd_tbls_rd_v.vz2_segmt_dim_ref v
ON (d.vz2_segmt_cd = v.vz2_segmt_cd  AND v.curr_prev_ind = 'C')
WHERE rpt_mth = '2024-09-01'
AND activity_cd IN ('DE','D3')
AND voluntary_disconnects >0
AND line_type_cd NOT IN  ( 'H','O','T' )
AND upper(sor_id) = 'V'
and COALESCE(upper(PREPAID_IND), 'N')= 'N'
AND v.vz2_segmt_ctgry_desc = 'Wireless Consumer Revenue'
AND data_tier  IN ('Smartphone','Basic')
AND upper(vsn_cust_type_cd) IN ('BE','PE')
--GROUP BY 1,2,3,4,5,tenure,pplan_cd,device_prod_nm,change_reas_cd,curr_contract_term,port_status_cd;"
