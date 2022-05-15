/**********************************************************************

	Name: 02_clean_sample_data.do

	Purpose: This code file takes the compiled student data from the last
	file and processes it into a structure amenable to job quality analyses
	in the next few codefiles (03x_ files).
	
	This code file pulls in another data file ("$project_data/employer_name_crosswalk_v1.xlsx")
	that was produced in a separate process to match employer names as they appear
	in our employment UI data and in the Burning Glass data.
	
	This code also pulls in a number of other data files we produced to
	better track the geography of counties and institutions in our context:
		-"$project_data/Virginia_City_County_Boundaries.csv"
		-"$project_data/VCCS County Crosswalk with Locations.csv"
		-"$project_data/vccs_geojson_main_campuses.csv"
		
	Finally, we use a publicly available crosswalk between MSAs and counties across the US:
		-"$project_data/qcew-county-msa-csa-crosswalk-csv.csv"
		
***********************************************************************/

	zipuse "$project_data/01_employment_aas.dta.zip", clear
	
	//Deal with employment data version discrepancies, as well as duplicate wage entries
		rename wage w
		rename qtr_tq emp_quarter_tq
		
	//Drop out erroneously duplicated UI observations
		gsort vccsid curr emp_quarter -w employer_name
	
		drop if vccsid==vccsid[_n-1] & curr==curr[_n-1] & employer_address==employer_address[_n-1] & employer_name==employer_name[_n-1] & w==w[_n-1] & emp_quarter==emp_quarter[_n-1]
		drop if vccsid==vccsid[_n-1] & curr==curr[_n-1] & w==w[_n-1] & emp_quarter==emp_quarter[_n-1]
		
	//Merge in cleaned employer names at the VCCS level (to consolidate)
		gen employer_name_vccs=ustrupper(employer_name)
		
		preserve
			import excel "$project_data/employer_name_crosswalk_v1.xlsx", firstrow clear
			keep employer_name_vccs_clean employer_name_vccs
			
			gduplicates drop
			
			tempfile name_merge
			save `name_merge', replace
		restore
		
		merge m:1 employer_name_vccs using `name_merge', nogen keep(match master)
		
		replace employer_name_vccs_clean=employer_name if employer_name_vccs_clean==""
		
		rename employer_name employer_name_raw
		
		rename employer_name_vccs_clean employer_name
		
	//Get unique pair counts
		gegen unique_pair=tag(employer_name vccsid)
		gegen quarter_count=tag(employer_name vccsid emp_quarter_tq)
		
		gegen unique_pairs=total(unique_pair), by(employer_name curr)
		
		
	//Set up data for tsset
		gegen pairing_id = group(panel_id employer_name)
		
		//Catch last set of duplicates
			gsort pairing_id emp_quarter_tq -w
			gegen dupe=tag(pairing_id emp_quarter_tq)
			drop if dupe==0
		
		tsset pairing_id emp_quarter_tq, quarterly
	
	//Merge in enrollment data
		merge m:1 vccsid emp_quarter_tq using "$project_data/01_student_build.dta", keep(match master) gen(enrollment_merge)
	
		drop taking_classes_flag_other taking_classes_flag_vccs
		replace taking_classes_flag=0 if taking_classes_flag==.
	
	//Merge in grad data
		merge m:1 panel_id emp_quarter_tq using "$project_data/01_degrees_over_time.dta", keep(match master) gen(nsc_merge)
		
		foreach var of varlist add_deg* {
			replace `var'=0 if `var'==.
		}

	//Set up spells for employment and other basic time-based predictors
		//Quarters since graduation
			gen qtrs_since_grad = emp_quarter_tq - grad_quarter_tq
		
		//Quarters of overall employment since graduation
			preserve
				gen employed=1
				gcollapse (max) employed, by(panel_id emp_quarter_tq)
				
				gsort panel_id emp_quarter_tq
				gen employed_qtrs_since_grad = 0
				by panel_id: replace employed_qtrs_since_grad=employed_qtrs_since_grad[_n-1] + employed[_n-1] if employed[_n-1]==1
			
				keep panel_id emp_quarter_tq employed_qtrs_since_grad
				
				tempfile tmp
				save `tmp', replace
			restore
		
			merge m:1 panel_id emp_quarter_tq using `tmp', keep(match master) gen(experience_merge)
		
		//Quarters at specific employer
			preserve
				gen employed=1
				gcollapse (max) employed, by(panel_id emp_quarter_tq employer_name)
				
				gsort panel_id employer_name emp_quarter_tq
				gen employed_qtrs_at_emp = 0
				by panel_id employer_name: replace employed_qtrs_at_emp=employed_qtrs_at_emp[_n-1] + employed[_n-1] if employed[_n-1]==1
			
				keep panel_id employer_name emp_quarter_tq employed_qtrs_at_emp
				
				tempfile tmp
				save `tmp', replace
			restore
		
			merge m:1 panel_id employer_name emp_quarter_tq using `tmp', keep(match master) gen(tenure_merge)

		//Create indicators for 1st job, 2nd job, 3rd job+ in a given quarter
			gsort panel_id emp_quarter_tq -w
			gen job1=0
			by panel_id emp_quarter_tq: replace job1=1 if job1[_n-1]==.
			gen job2=0
			by panel_id emp_quarter_tq: replace job2=1 if job1[_n-1]==1
			gen job3plus=0
			by panel_id emp_quarter_tq: replace job3=1 if job2!=1 & job1==0
		
		//Calculate length of specific spell
			gsort pairing_id emp_quarter_tq
			tsspell pairing_id
			
			gegen continuous_emp_prep = max(_seq), by(pairing_id _spell)
			gen continuous_emp_4q = (continuous_emp_prep>=4)
			gen continuous_emp_8q = (continuous_emp_prep>=8)
			
			gegen spell_quarter_max=max(emp_quarter_tq), by(pairing_id _spell)
			format spell_quarter_max %tq
		
		//Create indicators for first and last quarters of a given employment spell?
			gen firstlast=0
			replace firstlast=1 if _seq==1 | _end==1
			gen first_emp_spell_quarter=emp_quarter_tq if _seq==1
			gen last_emp_spell_quarter=emp_quarter_tq if _end==1
			
		//Living wage indicator for the given quarter
			//Take SCHEV methodology above; 13 weeks in a quarter at 35hrs per week at $15/hr
			gen lwe_quarter=0
			replace lwe_quarter=1 if w >= 6825 & w!=.
			
		//Encode employer names
			encode employer_name, gen(employer_name_num)
			
		//Encode panel_ids
			gegen panel_id_num = group(panel_id)
	
		//Destring curr nums
			destring curr, gen(curr_num)
			
		//Factor colleges
			encode college, gen(college_num)
			
		//Factor college*curr combos
			gen coll_curr=college+curr_text
			encode coll_curr, gen(coll_curr_num)
			drop coll_curr
	
		//Encode employer-curr combos
			gegen employer_curr_num = group(employer_name curr)
			gen employer_curr=employer_name+"_"+curr
	
	//Merge in financial aid data
		merge m:1 panel_id using "$project_data/01_finaid_build.dta", nogenerate keep(match master)
		
		foreach var of varlist pell_prior_exit_pen pell_prior_exit_ult {
			replace `var'=0 if `var'==.
			replace `var'_miss=1 if `var'_miss==.
		}

		//Gen final pell_prior_exit variable
		gen pell_prior_exit = (pell_prior_exit_pen==1 | pell_prior_exit_ult==1)
		gen pell_prior_exit_miss = (pell_prior_exit_pen_miss==1 & pell_prior_exit_ult_miss==1)

	//Merge in student demo data
		merge m:1 vccsid using "$project_data/01_student_demo_build.dta", nogenerate keep(match master)
		drop ceeb*

		//Clean up and process geographic data of student's county and college county
		replace juris_text="" if juris_instate!=1
			
		replace juris_text="Halifax" if juris_text=="South Boston"
		replace juris_text="Alleghany" if juris_text=="Clifton Forge"
		replace juris_text="" if juris_text=="In-State Unknown"
		
		rename juris_text home_county_name

		//Need to generate correct fips code from juris variable, which seems totally random
		preserve
			import delimited using "$project_data/Virginia_City_County_Boundaries.csv", clear
			
			keep name namelsad geoid
			
			//Manual fixing of names to facilitate merge
			replace name="Richmond City" if namelsad=="Richmond city"
			replace name="Roanoke City" if namelsad=="Roanoke city"
			replace name="Fairfax City" if namelsad=="Fairfax city"
			replace name="Bedford City" if namelsad=="Bedford city"
			replace name="Franklin City" if namelsad=="Franklin city"
			replace name="King & Queen" if name=="King and Queen"
			replace name="Isle Of Wight" if name=="Isle of Wight"
			
			tostring geoid, gen(home_county)
			
			rename name home_county_name
			
			keep home_county_name home_county

			
			tempfile namefips
			save `namefips', replace
		restore

		merge m:1 home_county_name using `namefips', keep(match master) nogen
		
		//Merge in county center coordinates for home_county
		preserve
			import delimited using "$project_data/Virginia_City_County_Boundaries.csv", clear
			
			keep geoid intptlat intptlon
			tostring geoid, gen(home_county)
			drop geoid
			
			rename intptlat home_lat
			rename intptlon home_lon
			
			tempfile homecoords
			save `homecoords', replace
		restore
		
		merge m:1 home_county using `homecoords', keep(match master) nogen
		
		//Merge in college county
		preserve
			import delimited using "$project_data/VCCS County Crosswalk with Locations.csv", clear
			
			keep if collegelocation==1
			keep fips vccscollege locality
			rename vccscollege college
			rename locality college_county_name
			tostring fips, gen(college_county)
			drop fips
			
			tempfile collegecounties
			save `collegecounties', replace
		restore
		
		merge m:1 college using `collegecounties', keep(match master) nogen
		
		//Merge in college location 
		preserve
			import delimited using "$project_data/vccs_geojson_main_campuses.csv", clear
			
			keep instnm latitude longitude
			
			rename instnm college
			rename latitude college_lat
			rename longitude college_lon
			
			split college, parse(" Community College")
			
			drop college college2
			rename college1 college
			
			replace college="Dabney S. Lancaster" if college=="Dabney S Lancaster"
			replace college="J. Sargeant Reynolds" if college=="J Sargeant Reynolds"
			replace college="Paul D. Camp" if college=="Paul D Camp"
			
			tempfile collegecoords
			save `collegecoords', replace
		restore
		
		merge m:1 college using `collegecoords', keep(match master) nogen

		//Merge in MSA areas for both college and home
		preserve
			import delimited using "$project_data/qcew-county-msa-csa-crosswalk-csv.csv", clear
			
			tostring countycode, gen(home_county)
			
			keep home_county msacode msatitle
			rename msacode home_msa_code
			rename msatitle home_msa_title
			
			tempfile homemsas
			save `homemsas', replace
			
			rename home_county college_county
			rename home_msa_code college_msa_code
			rename home_msa_title college_msa_title
			
			tempfile collegemsas
			save `collegemsas', replace
		restore
		
		merge m:1 home_county using `homemsas', keep(match master) nogenerate
		merge m:1 college_county using `collegemsas', keep(match master) nogenerate
		
		
	//Generate clean demo data
		gen female=.
		replace female=1 if gender=="F"
		replace female=0 if gender=="M"
		gen female_miss=(female==.)
		replace female=0 if female==.
		
		destring new_race, replace
		gen non_white=.
		replace non_white=1 if new_race!=1 & new_race!=0 & new_race!=7 & new_race!=.
		replace non_white=0 if new_race==1
		gen non_white_miss=(non_white==.)
		replace non_white=0 if non_white==.
	
		gen in_state=.
		replace in_state=1 if residency=="IS"
		replace in_state=0 if residency=="OS"
		gen in_state_miss=(in_state==.)
		replace in_state=0 if in_state==.
		
		gen visa2 = 0
		replace visa2 = 1 if visa!=""
		drop visa
		rename visa2 visa
		
		gen military = 1
		replace military = 0 if inlist(mil_status,"2","X","S","R")
		replace military = . if inlist(mil_status,"","1")
		drop mil_status
		gen military_miss=(military==.)
		replace military=0 if military==.
		
		gen first_gen = .
		replace first_gen = 1 if (mhe==1 | mhe==2 | mhe==3 | mhe==4) & (fhe==1 | fhe==2 | fhe==3 | fhe==4) 
		replace first_gen = 0 if (mhe==5 | mhe==6 | mhe==7) | (fhe==5 | fhe==6 | fhe==7) 
		gen first_gen_miss=(first_gen==.)
		replace first_gen=0 if first_gen==.
		
	//Clean up CIP codes for merging with occupation crosswalk later
		tostring cip, replace
		replace cip=cip+".0000" if strpos(cip, ".")==0
		split cip, parse(".")
		gen cip2_len=strlen(cip2)
		
		replace cip=cip+"0" if cip2_len==3
		replace cip=cip+"00" if cip2_len==2
		
		drop cip1 cip2 cip2_len
		
		split cip, parse(".")
		
		rename cip cip6
		
		replace cip2=substr(cip2, 1, 2)
		
		gen cip4=cip1+"."+cip2
		
		drop cip1 cip2
	
	//Save dataset
		zipsave "$project_data/02_employer_quality_clean.dta.zip", replace
		

		
		