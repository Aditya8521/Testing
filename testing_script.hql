-- change the variables according to the object. set command creates a variable and assigns value to it.

set hist_table = ea_common.gnrl_ldgr_dcmt_ln_itm_history_new;
set raw_table  = ea_common.gnrl_ldgr_dcmt_ln_itm_rw_new;
set ref_table  = ea_common.gnrl_ldgr_dcmt_ln_itm_ref_new;
set cons_table = ea_common.gnrl_ldgr_dcmt_ln_itm_dmnsn_enr_new;
set err_table  = ea_common.gnrl_ldgr_dcmt_ln_itm_err;
set fl_nm      = GL Documents - Line Items Incremental;
set ld_jb_nr   = EA 1.1b-031;
set src_sys_ky = 1;
-- set extra_whr_clause = AND (acct_typ_cd='D' OR acct_typ_cd='S') 

-- The below property prints the column names while running the query.
set hive.cli.print.header=true ;

set hive.execution.engine=tez;

select '${hiveconf:payload_id}' as Payload ;
select '${hiveconf:date}' as Date_value;


!echo;
!echo *************************************************************************************; ;
!echo Step 1: Check count of records transferred from history layer to raw layer;
!echo *************************************************************************************;
!echo;

SELECT  COUNT(*) as  hist_count FROM ${hiveconf:hist_table} WHERE payload like  '%${hiveconf:payload_id}%';

SELECT  concat(${hiveconf:raw_table},"raw_count:",count(*)) as raw_count FROM ${hiveconf:raw_table} WHERE src_sys_btch_nr ='${hiveconf:payload_id}' and date(ins_gmt_ts)='${hiveconf:date}';

!echo;
!echo *************************************************************************************;
!echo Step 2: Validate the records in the error  layer tables;
!echo *************************************************************************************;
!echo ;

SELECT  count(*) as error_count FROM ${hiveconf:err_table};

!echo;
!echo *************************************************************************************;
!echo Step 3: Validate the Audit fields in the raw layer tables ;
!echo *************************************************************************************;
!echo ;

-- Command  to check audit fields for raw layer tables:

SELECT COUNT(*) as raw_audit_count FROM ${hiveconf:raw_table} WHERE src_sys_ky = "${hiveconf:src_sys_ky}" AND lgcl_dlt_ind IS NULL AND TRIM(fl_nm) = "${hiveconf:fl_nm}" AND ld_jb_nr = "${hiveconf:ld_jb_nr}" AND src_sys_upd_ts IS NULL AND UNIX_TIMESTAMP(ins_gmt_ts,'yyyy-MM-dd') != 0 AND upd_gmt_ts IS NULL AND src_sys_extrc_gmt_ts IS NULL and  date(ins_gmt_ts)="${hiveconf:date}" and src_sys_btch_nr="${hiveconf:payload_id}" AND (acct_typ_cd='D' OR acct_typ_cd='S') ;


--HQL to check the values populated for the audit fields of a particular payload_id:

select src_sys_ky,lgcl_dlt_ind,fl_nm,ld_jb_nr,src_sys_upd_ts,ins_gmt_ts,upd_gmt_ts,src_sys_extrc_gmt_ts   from ${hiveconf:raw_table} where src_sys_btch_nr="${hiveconf:payload_id}";

!echo;
!echo *************************************************************************************;
!echo Step 4: Check #of records transferred from Raw layer to Refined layer tables;
!echo *************************************************************************************;
!echo;

-- Obtain count of records from raw layer tables executing following command:
SELECT COUNT(*) as raw_count FROM ${hiveconf:raw_table} where src_sys_btch_nr="${hiveconf:payload_id}" and date(ins_gmt_ts)="${hiveconf:date}" AND (acct_typ_cd='D' OR acct_typ_cd='S');

-- Obtain count of records from refined layer tables executing following command:
SELECT COUNT(*) as ref_count FROM ${hiveconf:ref_table} where src_sys_btch_nr="${hiveconf:payload_id}" and date(ins_gmt_ts)="${hiveconf:date}" AND (acct_typ_cd='D' OR acct_typ_cd='S');

!echo ;
!echo *************************************************************************************;
!echo Step 5: Validate Audit fields in refined layer;
!echo *************************************************************************************;
!echo;

-- Execute following command to check audit fields for refined layer

SELECT COUNT(*)  as ref_audit_count FROM ${hiveconf:ref_table} WHERE src_sys_ky = "${hiveconf:src_sys_ky}" AND lgcl_dlt_ind IS NULL AND fl_nm = "${hiveconf:fl_nm}" AND ld_jb_nr ="${hiveconf:ld_jb_nr}" AND src_sys_upd_ts IS NULL AND upd_gmt_ts IS NULL AND src_sys_extrc_gmt_ts IS NULL AND src_sys_btch_nr=${hiveconf:payload_id} AND date(ins_gmt_ts)='${hiveconf:date}' AND (acct_typ_cd='D' OR acct_typ_cd='S');

-- HQL to check the values populated for the audit fields of a particular payload_id:

Select src_sys_ky,lgcl_dlt_ind,fl_nm,ld_jb_nr,src_sys_upd_ts,ins_gmt_ts,upd_gmt_ts,src_sys_extrc_gmt_ts from ${hiveconf:ref_table} where src_sys_btch_nr=${hiveconf:payload_id};

!echo;
!echo *************************************************************************************;
!echo Step 6: Check count of records transferred from Refined layer to Provisioning layer;
!echo *************************************************************************************;
!echo;

--Obtain count of records from Provisioning layer table by executing following command:
SELECT COUNT(*) as cons_count FROM ${hiveconf:cons_table} where src_sys_btch_nr=${hiveconf:payload_id} and date(ins_gmt_ts)='${hiveconf:date}' AND (acct_typ_cd='D' OR acct_typ_cd='S');

!echo;
!echo *************************************************************************************;
!echo Step 7: Validate Audit fields in Provisioning layer;
!echo *************************************************************************************;
!echo;

--Execute following command to check audit fields for provisioning layer
SELECT COUNT(*) as cons_audit_count FROM ${hiveconf:cons_table} WHERE src_sys_ky = "${hiveconf:src_sys_ky}" AND lgcl_dlt_ind IS NULL AND fl_nm = "${hiveconf:fl_nm}" AND ld_jb_nr ="${hiveconf:ld_jb_nr}" AND src_sys_upd_ts IS NULL AND upd_gmt_ts IS NULL AND src_sys_extrc_gmt_ts IS NULL  AND date(ins_gmt_ts)='${hiveconf:date}' and src_sys_btch_nr="${hiveconf:payload_id}" AND (acct_typ_cd='D' OR acct_typ_cd='S');

!echo;
!echo *************************************************************************************;
!echo Step 8: Validate DDL with TDS specifications;
!echo *************************************************************************************;
!echo;

set raw_table;
!echo TABLE_COLS_BEGIN
desc ${hiveconf:raw_table};
!echo TABLE_COLS_END
!echo *************************************************************************************;
set ref_table;
desc ${hiveconf:ref_table};
!echo *************************************************************************************;
set cons_table;
desc ${hiveconf:cons_table};

!echo;
!echo *************************************************************************************;
!echo Step 9: Validate sample data;
!echo *************************************************************************************;
!echo;

select * from ${hiveconf:cons_table} limit 25;








