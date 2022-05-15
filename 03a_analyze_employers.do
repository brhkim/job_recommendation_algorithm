/**********************************************************************

	Name: 03a_analyze_employers.do

	Purpose: Conduct analysis on employer quality across varying
	specifications (e.g., college-level versus college-by-program level)
	using employment data generated in 02_clean_sample_data.do

		
***********************************************************************/

	zipuse "$project_data/02_employer_quality_clean.dta.zip", clear
	
	set seed 2021

	//Looping through to create indices for all graduate / college / program / college*program employer quality estimates
	
	//First, all
	preserve
		//Get unique pair counts
		drop unique_pair unique_pairs
		gegen unique_pair=tag(employer_name vccsid)
		
		gegen unique_pairs=total(unique_pair), by(employer_name)
	
		reghdfe w qtrs_since_grad taking_classes_flag add_deg* ///
			employed_qtrs_since_grad employed_qtrs_at_emp job1 job2 job3plus firstlast, ///
			absorb(w_fe=i.employer_name_num i.emp_quarter_tq) vce(cluster employer_curr_num)
		
		reghdfe lwe_quarter qtrs_since_grad taking_classes_flag add_deg* ///
			employed_qtrs_since_grad employed_qtrs_at_emp job1 job2 job3plus firstlast, ///
			absorb(lwe_fe=i.employer_name_num i.emp_quarter_tq) vce(cluster employer_curr_num)
		
		//Drop out right-censored spells
		reghdfe continuous_emp_8q qtrs_since_grad taking_classes_flag add_deg* ///
			employed_qtrs_since_grad employed_qtrs_at_emp job1 job2 job3plus firstlast ///
			if (spell_quarter_max!=tq(2019q4) | continuous_emp_8q==1), ///
			absorb(emp_8q_fe=i.employer_name_num i.emp_quarter_tq i._spell) vce(cluster employer_curr_num)
			
		gcollapse (max) unique_pairs w_fe lwe_fe emp_8q_fe, by(employer_name)
		
		//Drop out cells that are too small to be reliable
		keep if unique_pairs>=10
		
		foreach var of varlist w_fe lwe_fe emp_8q_fe {
			gegen `var'_all=std(`var')
		}
		
		drop w_fe lwe_fe emp_8q_fe
		
		save "$project_data/03a_fe_estimates_all.dta", replace
	restore
	

	//Second, college
	preserve
		//Get unique pair counts
		drop unique_pair unique_pairs
		gegen unique_pair=tag(employer_name vccsid college)
		
		gegen unique_pairs=total(unique_pair), by(employer_name college)
	
		reghdfe w qtrs_since_grad taking_classes_flag add_deg* ///
			employed_qtrs_since_grad employed_qtrs_at_emp job1 job2 job3plus firstlast, ///
			absorb(w_fe=i.employer_name_num#i.college_num i.emp_quarter_tq) vce(cluster employer_curr_num)
		
		reghdfe lwe_quarter qtrs_since_grad taking_classes_flag add_deg* ///
			employed_qtrs_since_grad employed_qtrs_at_emp job1 job2 job3plus firstlast, ///
			absorb(lwe_fe=i.employer_name_num#i.college_num i.emp_quarter_tq) vce(cluster employer_curr_num)
		
		//Drop out right-censored spells
		reghdfe continuous_emp_8q qtrs_since_grad taking_classes_flag add_deg* ///
			employed_qtrs_since_grad employed_qtrs_at_emp job1 job2 job3plus firstlast ///
			if (spell_quarter_max!=tq(2019q4) | continuous_emp_8q==1), ///
			absorb(emp_8q_fe=i.employer_name_num#i.college_num i.emp_quarter_tq i._spell) vce(cluster employer_curr_num)
		
		gcollapse (max) unique_pairs w_fe lwe_fe emp_8q_fe, by(employer_name college)
		
		//Drop out cells that are too small to be reliable
		keep if unique_pairs>=10
		
		foreach var of varlist w_fe lwe_fe emp_8q_fe {
			gegen `var'_coll=std(`var')
		}
		
		drop w_fe lwe_fe emp_8q_fe
		
		save "$project_data/03a_fe_estimates_coll.dta", replace
	restore
	

	//Third, program
	preserve
		//Get unique pair counts
		drop unique_pair unique_pairs
		gegen unique_pair=tag(employer_name vccsid curr)
		
		gegen unique_pairs=total(unique_pair), by(employer_name curr)
	
		reghdfe w qtrs_since_grad taking_classes_flag add_deg* ///
			employed_qtrs_since_grad employed_qtrs_at_emp job1 job2 job3plus firstlast, ///
			absorb(w_fe=i.employer_name_num#i.curr_num i.emp_quarter_tq) vce(cluster employer_curr_num)
		
		reghdfe lwe_quarter qtrs_since_grad taking_classes_flag add_deg* ///
			employed_qtrs_since_grad employed_qtrs_at_emp job1 job2 job3plus firstlast, ///
			absorb(lwe_fe=i.employer_name_num#i.curr_num i.emp_quarter_tq) vce(cluster employer_curr_num)
		
		//Drop out right-censored spells
		reghdfe continuous_emp_8q qtrs_since_grad taking_classes_flag add_deg* ///
			employed_qtrs_since_grad employed_qtrs_at_emp job1 job2 job3plus firstlast ///
			if (spell_quarter_max!=tq(2019q4) | continuous_emp_8q==1), ///
			absorb(emp_8q_fe=i.employer_name_num#i.curr_num i.emp_quarter_tq i._spell) vce(cluster employer_curr_num)
		
		gcollapse (max) unique_pairs w_fe lwe_fe emp_8q_fe, by(employer_name curr)
		
		//Drop out cells that are too small to be reliable
		keep if unique_pairs>=5
		
		foreach var of varlist w_fe lwe_fe emp_8q_fe {
			gegen `var'_curr=std(`var')
		}
		
		drop w_fe lwe_fe emp_8q_fe
		
		save "$project_data/03a_fe_estimates_curr.dta", replace
	restore
	

	//Last, college*program
	preserve
		//Get unique pair counts
		drop unique_pair unique_pairs
		gegen unique_pair=tag(employer_name vccsid college curr)
		
		gegen unique_pairs=total(unique_pair), by(employer_name college curr)
	
		reghdfe w qtrs_since_grad taking_classes_flag add_deg* ///
			employed_qtrs_since_grad employed_qtrs_at_emp job1 job2 job3plus firstlast, ///
			absorb(w_fe=i.employer_name_num#i.curr_num#i.college_num i.emp_quarter_tq) vce(cluster employer_curr_num)
		
		reghdfe lwe_quarter qtrs_since_grad taking_classes_flag add_deg* ///
			employed_qtrs_since_grad employed_qtrs_at_emp job1 job2 job3plus firstlast, ///
			absorb(lwe_fe=i.employer_name_num#i.curr_num#i.college_num i.emp_quarter_tq) vce(cluster employer_curr_num)
		
		//Drop out right-censored spells
		reghdfe continuous_emp_8q qtrs_since_grad taking_classes_flag add_deg* ///
			employed_qtrs_since_grad employed_qtrs_at_emp job1 job2 job3plus firstlast ///
			if (spell_quarter_max!=tq(2019q4) | continuous_emp_8q==1), ///
			absorb(emp_8q_fe=i.employer_name_num#i.curr_num#i.college_num i.emp_quarter_tq i._spell) vce(cluster employer_curr_num)
			
		gcollapse (max) unique_pairs w_fe lwe_fe emp_8q_fe, by(employer_name college curr)
		
		//Drop out cells that are too small to be reliable
		keep if unique_pairs>=5
		
		foreach var of varlist w_fe lwe_fe emp_8q_fe {
			gegen `var'_collcurr=std(`var')
		}
		
		drop w_fe lwe_fe emp_8q_fe
		
		save "$project_data/03a_fe_estimates_collcurr.dta", replace
	restore
	
	
	// Experiment with Josh Goodman's suggestion to collapse all of this stuff into a more straightforward index: how much did someone make in the three years following their first employment with a given employer just out of graduation?
	preserve
		//Sort the data first
		gsort panel_id emp_quarter_tq -w
		
		//Figure out which employer was their first primary employer after graduation
		gen first_emp_check = (employed_qtrs_since_grad==0 & employed_qtrs_at_emp==0 & job1==1)
		gen first_emp_check_qtr = emp_quarter_tq if first_emp_check==1
			
		egen first_emp_qtr=mean(first_emp_check_qtr), by(panel_id)
		
		//Now get rid of any data outside of three years after their first employed quarter with their first primary employer
		keep if emp_quarter_tq-first_emp_qtr<=12
			
		//Get the name of their first employer
		gen first_employer_check=employer_name if employed_qtrs_since_grad==0 & employed_qtrs_at_emp==0 & job1==1
		egen first_employer=mode(first_employer_check), by(panel_id)
			
		//Collapse down to the student-by-first_employer level
		gcollapse (sum) w (mean) first_emp_qtr, by(panel_id first_employer)

		//Last quarter of data available is 2019q4, which is equivalent to 239 in raw quarter count.
		//Drop out any obs where there hasn't been enough time to track 3 yrs after the first quarter of employment
		keep if 239-first_emp_qtr>=12
			
		//Get a count of how many students are included in each count
		gen counter=1
			
		//Collapse down to the employer level
		gcollapse (sum) counter (mean) w, by(first_employer)
			
		//Get rid of small cell-sizes
		keep if counter>=5
		
		rename first_employer employer_name
		rename counter jg_unique_pairs
		rename w jg_estimate
		
		foreach var of varlist jg_estimate {
			gegen `var'_all=std(`var')
		}
			
		save "$project_data/03a_jg_estimates.dta", replace
	restore
	
	//Compare results to see how stable any of these are compared to one another
		use "$project_data/03a_fe_estimates_all.dta", clear
		
		merge 1:1 employer_name using "$project_data/03a_jg_estimates.dta"
		
		keep if _merge==3
		
		gsort -w_fe_all
		gen rank_w_fe_all = _n
		
		gsort -jg_estimate_all
		gen rank_jg_estimate_all = _n
		
		gen diff=rank_w_fe_all-rank_jg_estimate_all
		gen raw_diff=abs(w_fe_all-jg_estimate_all)
		