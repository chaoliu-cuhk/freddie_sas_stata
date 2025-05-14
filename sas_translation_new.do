clear all
set more off

global DATADIR "./freddie/raw_data"
global OUTDIR "./freddie/after_sas"
global TEMPDIR "./freddie/temp"

*****************************************************************************
**# quarterly files #
forv yr = 1999/2020 {
	
	forv q = 1/4 {
		
		**# read performance #
		* Read svc data
		import delimited using "$DATADIR/historical_data_`yr'Q`q'/historical_data_time_`yr'Q`q'.txt", delimiter("|") clear
		rename v1 id_loan
		rename v2 period
		rename v3 curr_act_upb
		rename v4 delq_sts
		rename v5 loan_age
		rename v6 mths_remng
		rename v7 dt_dfct_setlmt
		rename v8 flag_mod
		rename v9 cd_zero_bal
		rename v10 dt_zero_bal
		rename v11 cur_int_rt
		rename v12 cur_dfrd_upb
		rename v13 dt_lst_pi
		rename v14 mi_recoveries
		rename v15 net_sale_proceeds
		rename v16 non_mi_recoveries
		rename v17 expenses
		rename v18 legal_costs
		rename v19 maint_pres_costs
		rename v20 taxes_ins_costs
		rename v21 misc_costs
		rename v22 actual_loss
		rename v23 modcost
		rename v24 stepmod_ind
		rename v25 dpm_ind
		rename v26 eltv
		rename v27 zb_removal_upb
		rename v28 dlq_acrd_int
		rename v29 disaster_area_flag
		rename v30 borr_assist_ind
		rename v31 monthly_modcost
		rename v32 amt_int_brng_upb
		sort id_loan period
		capture destring cd_zero_bal, replace force
		capture tostring flag_mod, replace force
		save "$TEMPDIR/svcg_`yr'Q`q'.dta", replace

		**# read origination #
		* Read orig data 
		import delimited using "$DATADIR/historical_data_`yr'Q`q'/historical_data_`yr'Q`q'.txt", delimiter("|") clear
		rename v1 fico
		rename v2 dt_first_pi
		rename v3 flag_fthb
		rename v4 dt_matr
		rename v5 cd_msa
		rename v6 mi_pct
		rename v7 cnt_units
		rename v8 occpy_sts
		rename v9 cltv
		rename v10 dti
		rename v11 orig_upb
		rename v12 ltv
		rename v13 orig_int_rt
		rename v14 channel
		rename v15 ppmt_pnlty
		rename v16 amrtzn_type
		rename v17 st
		rename v18 prop_type
		rename v19 zipcode
		rename v20 id_loan
		rename v21 loan_purpose
		rename v22 orig_loan_term
		rename v23 cnt_borr
		rename v24 seller_name
		rename v25 servicer_name
		rename v26 flag_sc
		rename v27 id_loan_preharp
		rename v28 ind_afdl
		rename v29 ind_harp
		rename v30 cd_ppty_val_type
		rename v31 flag_int_only
		cap rename v32 ind_mi_cncl
		sort id_loan
		save "$TEMPDIR/orig_`yr'Q`q'.dta", replace

		**# svcg_dtls #
		* Merge svc + orig -> svcg_dtls 
		use "$TEMPDIR/svcg_`yr'Q`q'.dta", clear
		gduplicates drop id_loan period, force
		merge m:1 id_loan using "$TEMPDIR/orig_`yr'Q`q'.dta", keepusing(orig_upb) keep(3) nogen

		* Compute lag/prior & delq_sts_new
		sort id_loan period
		by id_loan (period): gen lag_id_loan          = id_loan[_n-1]
		by id_loan (period): gen lag2_id_loan         = id_loan[_n-2]
		by id_loan (period): gen lag_curr_act_upb     = curr_act_upb[_n-1]
		by id_loan (period): gen lag_delq_sts         = delq_sts[_n-1]
		by id_loan (period): gen lag2_delq_sts        = delq_sts[_n-2]
		by id_loan (period): gen lag_period           = period[_n-1]
		by id_loan (period): gen lag_cur_int_rt       = cur_int_rt[_n-1]
		by id_loan (period): gen lag_non_int_brng_upb = cur_dfrd_upb[_n-1]

		by id_loan: gen first_obs = _n == 1
		by id_loan: gen second_obs = _n == 2
		gen prior_upb = cond(first_obs, 0, lag_curr_act_upb)
		gen prior_int_rt = cond(first_obs, cur_int_rt, lag_cur_int_rt)
		gen prior_delq_sts = cond(first_obs, "00", lag_delq_sts)
		gen prior_period = cond(first_obs, ., lag_period)
		gen prior_frb_upb = cond(first_obs, ., lag_non_int_brng_upb)
		gen prior_delq_sts_2 = "00" if first_obs == 1 | second_obs == 1
		replace prior_delq_sts_2 = lag2_delq_sts if id_loan == lag2_id_loan

		gen delq_sts_new = ""
		replace delq_sts_new = delq_sts if delq_sts != "RA"
		gen per_yyyy = floor(period/100)
		gen per_mm   = mod(period, 100)
		gen pr_yyyy = floor(prior_period/100)
		gen pr_mm   = mod(prior_period, 100)
		gen per_m = ym(per_yyyy, per_mm)
		gen pr_m  = ym(pr_yyyy, pr_mm)
		gen period_diff = per_m - pr_m
		replace delq_sts_new = "6" if delq_sts == "RA" & period_diff == 1 & prior_delq_sts == "5"
		replace delq_sts_new = "4" if delq_sts == "RA" & period_diff == 1 & prior_delq_sts == "3"
		replace delq_sts_new = "3" if delq_sts == "RA" & period_diff == 1 & prior_delq_sts == "2"

		drop lag_curr_act_upb lag2_id_loan lag_delq_sts lag2_delq_sts lag_period lag_cur_int_rt
		save "$TEMPDIR/svcg_dtls_`yr'Q`q'.dta", replace


		**# pop_i #
		* First-instance delinquencies (pop_*) 
		foreach d in 1 2 3 4 6 {
			
			use "$TEMPDIR/svcg_dtls_`yr'Q`q'.dta", clear
			sort id_loan period
			keep if delq_sts_new == "`d'"
			bys id_loan (period): keep if _n == 1
			gen dlq_ind_`d' = 1
			gen dlq_upb_`d' = curr_act_upb if !inlist(curr_act_upb, 0, .)
			replace dlq_upb_`d' = prior_upb if !inlist(prior_upb, 0, .) & inlist(curr_act_upb, 0, .)
			replace dlq_upb_`d' = orig_upb if inlist(prior_upb, 0, .) & inlist(curr_act_upb, 0, .)
			save "$TEMPDIR/pop_`d'_`yr'Q`q'.dta", replace

		}
		//append to pop_i_final


		**# pd180 #
		* D180 / pre-D180 default (pd180) 
		use "$TEMPDIR/svcg_dtls_`yr'Q`q'.dta", clear
		keep if delq_sts_new == "6"
		tempfile tmp
		save `tmp', replace

		use "$TEMPDIR/svcg_dtls_`yr'Q`q'.dta", clear
		sort id_loan period
		keep if inlist(cd_zero_bal, 2, 3, 15) | delq_sts == "RA"
		drop if real(delq_sts_new) >= 6 & delq_sts_new != ""

		append using `tmp'
		bys id_loan (period): keep if _n == 1
		gen pd_d180_ind = 1
		gen pd_d180_upb = cond(!inlist(curr_act_upb, 0, .), curr_act_upb, cond(!inlist(prior_upb, 0, .), prior_upb, orig_upb))
		save "$TEMPDIR/pd180_`yr'Q`q'.dta", replace
		//append to pd_d180

		**# modification #
		* Modification records (mod_loan)
		use "$TEMPDIR/svcg_dtls_`yr'Q`q'.dta", clear
		sort id_loan period
		keep if flag_mod == "Y"
		bys id_loan (period): keep if _n == 1
		gen mod_ind = 1
		gen mod_upb = cond(!inlist(curr_act_upb, 0, .), curr_act_upb, cond(!inlist(prior_upb, 0, .), prior_upb, orig_upb))
		save "$TEMPDIR/mod_loan_`yr'Q`q'.dta", replace
		//append to mod_rcd

		**# last obs #
		* Termination records (trm_rcd) 
		use "$TEMPDIR/svcg_dtls_`yr'Q`q'.dta", clear
		sort id_loan period
		bys id_loan (period): keep if _n == _N
		gen default_upb = . 
		replace default_upb = cond(!inlist(curr_act_upb, 0, .), curr_act_upb, cond(!inlist(prior_upb, 0, .), prior_upb, orig_upb)) if inlist(cd_zero_bal, 2, 3, 9, 15)
		gen current_int_rt = cond(!missing(cur_int_rt) & cur_int_rt != 0, cur_int_rt, prior_int_rt)
		save "$TEMPDIR/trm_rcd_`yr'Q`q'.dta", replace

		**# dflt #
		* Default details (dflt) 
		use "$TEMPDIR/trm_rcd_`yr'Q`q'.dta", clear
		keep if inlist(cd_zero_bal, 2, 3, 9, 15)

		gen dflt_delq_sts = ""
		replace dflt_delq_sts = delq_sts if inlist(cd_zero_bal, 2, 3, 15)
		replace dflt_delq_sts = prior_delq_sts if cd_zero_bal == 9 & prior_delq_sts != "RA"
		replace dflt_delq_sts = prior_delq_sts_2 if cd_zero_bal== 9 & prior_delq_sts == "RA"

		gen acqn_to_dispn = 0 if cd_zero_bal == 9 & prior_delq_sts != "RA"
		replace acqn_to_dispn = per_m - pr_m if cd_zero_bal == 9 & prior_delq_sts == "RA"

		destring dflt_delq_sts, force gen(dflt_delq_num)
		gen mths_dlq_dflt_dispn = dflt_delq_num + acqn_to_dispn
		gen mths_dlq_dflt_acqn = dflt_delq_num
		gen frb_upb = prior_frb_upb	
		save "$TEMPDIR/dflt_`yr'Q`q'.dta", replace

		**# all info #
		* Merge everything into all_orign_dtl
		use "$TEMPDIR/orig_`yr'Q`q'.dta", clear
		merge 1:1 id_loan using "$TEMPDIR/trm_rcd_`yr'Q`q'.dta", keepusing(current_int_rt dt_dfct_setlmt cd_zero_bal dt_zero_bal ///
			expenses mi_recoveries non_mi_recoveries net_sale_proceeds ///
			actual_loss legal_costs taxes_ins_costs maint_pres_costs misc_costs ///
			modcost dt_lst_pi delq_sts zb_removal_upb dlq_acrd_int prior_upb curr_act_upb) ///
			keep(1 3) nogen
		rename dt_zero_bal zero_bal_period
		rename delq_sts zero_bal_delq_sts

		merge 1:1 id_loan using "$TEMPDIR/pop_1_`yr'Q`q'.dta", keepusing(dlq_ind_1 dlq_upb_1) keep(1 3) nogen
		rename dlq_ind_1 dlq_ever30_ind
		rename dlq_upb_1 dlq_ever30_upb

		merge 1:1 id_loan using "$TEMPDIR/pop_2_`yr'Q`q'.dta", keepusing(dlq_ind_2 dlq_upb_2) keep(1 3) nogen
		rename dlq_ind_2 dlq_ever60_ind
		rename dlq_upb_2 dlq_ever60_upb

		merge 1:1 id_loan using "$TEMPDIR/pop_3_`yr'Q`q'.dta", keepusing(dlq_ind_3 dlq_upb_3) keep(1 3) nogen
		rename dlq_ind_3 dlq_everd90_ind
		rename dlq_upb_3 dlq_everd90_upb

		merge 1:1 id_loan using "$TEMPDIR/pop_4_`yr'Q`q'.dta", keepusing(dlq_ind_4 dlq_upb_4) keep(1 3) nogen
		rename dlq_ind_4 dlq_everd120_ind
		rename dlq_upb_4 dlq_everd120_upb

		merge 1:1 id_loan using "$TEMPDIR/pop_6_`yr'Q`q'.dta", keepusing(dlq_ind_6 dlq_upb_6) keep(1 3) nogen
		rename dlq_ind_6 dlq_everd180_ind
		rename dlq_upb_6 dlq_everd180_upb

		gen prepay_count = inlist(cd_zero_bal, 1, 96)
		gen default_count = inlist(cd_zero_bal, 2, 3, 9, 15)
		gen prepay_upb = cond(prepay_count == 1, prior_upb, .)
		gen rmng_upb = cond(!missing(curr_act_upb), curr_act_upb, .)

		merge 1:1 id_loan using "$TEMPDIR/mod_loan_`yr'Q`q'.dta", keepusing(mod_ind mod_upb) keep(1 3) nogen

		merge 1:1 id_loan using "$TEMPDIR/pd180_`yr'Q`q'.dta", keepusing(pd_d180_ind pd_d180_upb) keep(1 3) nogen

		sort id_loan
		gen orig_y = `yr'
		gen orig_q = `q'
		drop prior_upb curr_act_upb
		save "$OUTDIR/all_orign_dtl_`yr'Q`q'.dta", replace
		
		erase "$TEMPDIR/orig_`yr'Q`q'.dta"
		erase "$TEMPDIR/svcg_dtls_`yr'Q`q'.dta"
		erase "$TEMPDIR/svcg_`yr'Q`q'.dta"
		
	}
}

*****************************************************************************
**# aggregate files #

* Modification records (mod_rcd)
clear 
tempfile tmp
save `tmp', emptyok replace

forv yr = 1999/2023 {
	
	forv q = 1/4 {
		
		use "$TEMPDIR/mod_loan_`yr'Q`q'.dta", clear
		cap destring net_sale_proceeds, replace force
		cap tostring stepmod_ind, replace force
		append using `tmp'
		save `tmp', replace
	
	}

}
save "$OUTDIR/mod_rcd.dta", replace


* First-instance delinquencies (pop_*_final)
foreach d in 1 2 3 4 6 {
	
	clear 
	tempfile tmp
	save `tmp', emptyok replace

	forv yr = 1999/2023 {
		
		forv q = 1/4 {
			
			use "$TEMPDIR/pop_`d'_`yr'Q`q'.dta", clear
			cap destring net_sale_proceeds, replace force
			cap tostring stepmod_ind, replace force
			append using `tmp'
			save `tmp', replace
		
		}

	}
	
	save "$OUTDIR/pop_`d'_final.dta", replace

}

* D180 / pre-D180 default (pd_d180)
clear 
tempfile tmp
save `tmp', emptyok replace

forv yr = 1999/2023 {
	
	forv q = 1/4 {
		
		use "$TEMPDIR/pd180_`yr'Q`q'.dta", clear
		cap destring net_sale_proceeds, replace force
		cap tostring stepmod_ind, replace force
		append using `tmp'
		save `tmp', replace
	
	}

}
save "$OUTDIR/pd_d180.dta", replace

* Default (all_dflt)
clear 
tempfile tmp
save `tmp', emptyok replace

forv yr = 1999/2023 {
	
	forv q = 1/4 {
		
		use "$TEMPDIR/dflt_`yr'Q`q'.dta", clear
		cap destring net_sale_proceeds, replace force
		cap tostring stepmod_ind, replace force
		append using `tmp'
		save `tmp', replace
	
	}

}
save "$OUTDIR/all_dflt.dta", replace

shell rm -r "$TEMPDIR"




