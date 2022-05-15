/**********************************************************************

	Name: 01_compile_sample_data.do

	Purpose: Compile a dataset of historical student data (inclusive of their
	graduation data, financial aid data, employment history, and so on) to 
	estimate employer quality for use in job quality index calculations later.
	Basically all steps in this file are in service of putting together (eventually)
	an individual-quarter dataset with all relevant student covariates (e.g., when
	enrolled, whether they received financial aid, how many terms they've been
	employed, etc.).
	
	Our graduation data is at the student-term-degree level, listing each degree
	a student received. We collapse individual degrees down to a broader category
	(e.g., "Nursing" rather than "Emergency Nursing") to reduce dimensionality.
	
	Our enrollment data is structured at the student-term-campus level, listing
	demographic and enrollment data for each student, each term.
	
	Our employment UI data is structured at the individual-quarter-employer level,
	listing earnings and employer information but, at least in our data, very 
	little job data.
	
	Our financial aid data are structured at the student-year level, collecting
	term-level financial aid data in each row.
	
	Our NSC data are structured at the student-term-institution level, similar to
	how it would be structured on receipt with only minor cleaning steps done prior.
	
	Note that the last few datasets are constructed separately and are provided
	with this codebase given that they were pulled from public sources. Those files are:
		-"$project_data/qcew-county-msa-csa-crosswalk-csv.csv"
		-"$project_data/county_adjacency2010.dta"
		-"$project_data/Client Version - CIP2010 to BGOT 341.xlsx"

***********************************************************************/

	//Gather up all grad data for the relevant cohorts
		foreach cohort in $cohorts {
			zipuse "$raw_data/Build/Graduation/Graduation_`cohort'.dta.zip", clear
			
			capture tostring curr, replace
			
			keep vccsid acadyr college curr curr_text gpa degree cip lstterm lstyr
			
			tempfile grad_`cohort'_build
			save `grad_`cohort'_build', replace
		}
	
		//Merge files across each graduate cohort
			clear
			
			foreach cohort in $cohorts {
				append using `grad_`cohort'_build'
			}

		//Drop out seemingly duplicate degrees earned, taking earliest data if a conflict
			gsort vccsid acadyr
			egen dupecheck=tag(vccsid curr degree)
			drop if dupecheck==0
			drop dupecheck
			
		//Replace lstterm variable for quarter merging
			//Generate last quarter enrolled for post-graduation outcomes
			//Note there are no Q1 graduations due to academic calendar
			tostring lstterm, replace
			gen lstqt=.
			replace lstqt=2 if substr(lstterm,4,1)=="2"
			replace lstqt=3 if substr(lstterm,4,1)=="3"
			replace lstqt=4 if substr(lstterm,4,1)=="4"

			destring lstyr, replace
			gen grad_quarter = string(lstyr) + "q" + string(lstqt)
			// integer = 1 for 2005Q1, = 2 for 2005Q2, ... = 5 for 2006 Q1, etc.
			gen grad_quarter_tq=quarterly(grad_quarter, "YQ")
			format grad_quarter_tq %tq
			
			drop grad_quarter lstqt lstyr lstterm

			keep if degree=="AAS"
		
			zipsave "$project_data/01_grad_build_aas.dta.zip", replace
	

	//Bring together only relevant student enrollment data for demographic analysis and for enrollment after graduation analysis
		//Minor processing of grad build file to ensure proper merging with student build files
		zipuse "$project_data/01_grad_build_aas.dta.zip", clear
		keep vccsid
		duplicates drop vccsid, force
		tempfile temp
		save `temp', replace
		
		clear
		local filelist: dir "$raw_data/Build/Student" files "*.dta.zip"
		local filelist: list sort filelist
		foreach file of local filelist {
			di "`file'"
			preserve
				zipuse "$raw_data/Build/Student/`file'", clear
				
				//Keep only data for graduates we're looking at
				merge m:1 vccsid using `temp', keep(match) nogenerate
				
				save "$project_data/01_student_build.dta", replace
			restore
			
			append using "$project_data/01_student_build.dta"
		}
		
		//Generate proper quarters for merging
			tostring strm, replace
			gen strm_quarter = "20" + substr(strm, 2, 2) + "q" + substr(strm,4,1)
			// integer = 1 for 2005Q1, = 2 for 2005Q2, ... = 5 for 2006 Q1, etc.
			gen strm_quarter_tq=quarterly(strm_quarter, "YQ")
			format strm_quarter_tq %tq
			
		//Create a dataset for student demographic analysis
		preserve
				keep vccsid strm_quarter_tq citz_status fhe mhe gender new_race hisp_fl residency visa mil_status ceeb* zip_us hs_grad_year juris juris_instate juris_text
			
			//Temp fix for erroneous 2009Q3 dataset
				drop if strm_quarter_tq==tq(2009q3)
			
			
			//We'll keep only the most recent observation for the student in this case, so ensure completeness of data given 
			//by filling in with the most recent information available for each variable
				foreach var of varlist citz_status fhe mhe gender new_race hisp_fl residency visa mil_status  ceeb* zip_us hs_grad_year juris juris_instate juris_text {
					gsort vccsid strm_quarter_tq -`var'
					capture replace `var'=`var'[_n-1] if vccsid==vccsid[_n-1] & `var'=="" & `var'[_n-1]!=""
					capture replace `var'=`var'[_n-1] if vccsid==vccsid[_n-1] & `var'==. & `var'[_n-1]!=.
				}
				
				by vccsid: egen recent=max(strm_quarter_tq)
				keep if strm_quarter_tq==recent
				duplicates drop
			
			//For few remaining duplicates, drop the one with the least amount of info
				egen misscheck=rowmiss(citz_status fhe mhe gender new_race hisp_fl residency visa mil_status ceeb zip_us hs_grad_year juris juris_instate juris_text)
				by vccsid: egen missmin=min(misscheck)
				drop if missmin!=misscheck
				
			//If any dupes remain, drop randomly
				sort vccsid
				egen dupecheck = tag(vccsid)
				drop if dupecheck==0
			
				drop dupecheck missmin misscheck recent strm_quarter_tq
			
			save "$project_data/01_student_demo_build.dta", replace
		restore
			

		//Generate final enrollment variables for use and clean duplicates
			gen taking_classes_flag_vccs=1
			gen emp_quarter_tq=strm_quarter_tq
			
			keep vccsid taking_classes_flag_vccs emp_quarter_tq intended_degree
			
			duplicates drop vccsid emp_quarter_tq intended_degree, force
			
			//For anyone taking multiple degrees, take only the highest degree to drop down to individual by quarter level. Alphabetical sort works perfectly here
			gsort vccsid emp_quarter_tq intended_degree
			egen dupecheck = tag(vccsid emp_quarter_tq)
			drop if dupecheck==0
			drop dupecheck

		save "$project_data/01_student_build.dta", replace

	
	

	//Load in NSC data to capture enrollment behavior besides just at VCCS
		//Minor processing of grad build file to ensure proper merging with NSC build files
		zipuse "$project_data/01_grad_build_aas.dta.zip", clear
		keep vccsid
		duplicates drop vccsid, force
		tempfile temp
		save `temp', replace
		
		clear
		
		forvalues year = $firstyear/$lastyear {
			preserve
				zipuse "$raw_data/Build/NSC/NSC_enrollment_`year'.dta.zip", clear
				keep vccsid enrol_begin enrol_end
				merge m:1 vccsid using `temp', keep(match) nogenerate
				keep vccsid enrol_begin enrol_end
				gen year=`year'
				tostring year, replace
				
				tempfile temp2
 				save `temp2', replace
				display `year'
			restore
				
			append using `temp2'
 		}
 		
		//Generate string variables for NSC dates
			gen begin_month = month(enrol_begin)
			gen end_month = month(enrol_end)
			
		//Assign enrollment windows to quarters
			gen q1 = begin_month==1 | begin_month==2 | begin_month==3
			gen q2 = begin_month==4 | begin_month==5 | begin_month==6 | (begin_month<4 & end_month>3)
			gen q3 = begin_month==7 | begin_month==8 | begin_month==9 | (begin_month<7 & end_month>6)
			gen q4 = begin_month==10 | begin_month==11 | begin_month==12 | (begin_month<10 & end_month>9)
			
		//Reshape dataset to quarterly
			keep vccsid year q*
			
			collapse (max) q1 q2 q3 q4, by (vccsid year)
			
			reshape long q, i(vccsid year) j(quarter)

		//Generate proper quarter date variable
			gen emp_quarter = year + "q" + string(quarter)
			gen emp_quarter_tq=quarterly(emp_quarter, "YQ")
			format emp_quarter_tq %tq
			
		//Form into final dataset
			rename q taking_classes_flag_other
			keep if taking_classes_flag_other==1
			keep vccsid emp_quarter_tq taking_classes_flag_other
			
			duplicates drop vccsid emp_quarter_tq, force
			
			merge 1:1 vccsid emp_quarter_tq using "$project_data/01_student_build.dta", nogen
			
			replace taking_classes_flag_other=0 if taking_classes_flag_other==. & taking_classes_flag_vccs==1
			replace taking_classes_flag_vccs=0 if taking_classes_flag_vccs==. & taking_classes_flag_other==1
			
			gen taking_classes_flag = (taking_classes_flag_other==1 | taking_classes_flag_vccs==1)
			
			keep vccsid emp_quarter_tq taking_classes_*

		save "$project_data/01_student_build.dta", replace

	
	//Gather up all employment data in relevant colleges for the relevant cohorts
	//Set up initial employment file in time period
		//Minor processing of grad build file to ensure proper merging with student build files
		zipuse "$project_data/01_grad_build_aas.dta.zip", clear

		tempfile temp
		save `temp', replace
		
		clear
		
		local year = $firstyear
		local quarters q1 q2 q3 q4
		
		while `year' <= $lastyear {
			preserve
				foreach quarter of local quarters {
					zipuse "$raw_data/Build/Employment/Employment_`year'_`quarter'.dta.zip", clear
					
					keep vccsid qtr_tq wage_adjusted employer_address employer_name naics
					
					joinby vccsid using `temp', unmatched(none)
					
					keep if qtr_tq>=grad_quarter_tq

					tempfile temp_`quarter'
					save `temp_`quarter'', replace
				}
			restore
			
			foreach quarter of local quarters {
				append using `temp_`quarter''
			}
			
			local ++year
			display "`year'"
		}
		
		gsort vccsid qtr_tq wage_adjusted
		
		gen panel_id=vccsid+"_"+curr
		
		zipsave "$project_data/01_employment_aas.dta.zip", replace
		
		
		
		
		
	//Separately process additional degrees/certs data

		//Get sample crosswalks: panel_id
			zipuse "$project_data/01_employment_aas.dta.zip", clear
			keep vccsid curr grad_quarter_tq panel_id
				
			gduplicates drop
				
			save "$project_data/01_sample_panelid.dta", replace
	
		//Get sample crosswalks: vccsid
			keep vccsid
			gduplicates drop
			
			save "$project_data/01_sample_vccsid.dta", replace
			
		//Merge in relevant grad data
			//VCCS
				zipuse "${raw_data}/Master_student_x_term_data/term_level_grads_vccs.dta.zip", clear
				
				merge m:1 vccsid using "$project_data/01_sample_vccsid.dta", nogen keep(match)
				
				gen add_deg_vccs_dipl = degree_level_vccs=="Diploma"
				gen add_deg_vccs_cert = degree_level_vccs=="Certificate"
				gen add_deg_vccs_aa = degree_level_vccs=="Associate"
				
				//Generate proper quarters for merging
					tostring strm, replace
					gen strm_quarter = ""
					replace strm_quarter = "20" + substr(strm, 2, 2) + "q" + substr(strm,4,1) if substr(strm, 1, 1)=="2"
					replace strm_quarter = "19" + substr(strm, 2, 2) + "q" + substr(strm,4,1) if substr(strm, 1, 1)=="1"
					// integer = 1 for 2005Q1, = 2 for 2005Q2, ... = 5 for 2006 Q1, etc.
					gen grad_quarter_tq2=quarterly(strm_quarter, "YQ")
					format grad_quarter_tq2 %tq
				
					drop strm_quarter strm 
				
				keep vccsid grad_curr add_deg_* grad_quarter_tq2
				
				tempfile temp_vccs
				save `temp_vccs', replace
			
			//NSC
				zipuse "${raw_data}/Master_student_x_term_data/term_level_grads_nsc.dta.zip", clear
				
				merge m:1 vccsid using "$project_data/01_sample_vccsid.dta", nogen keep(match)
		
				gen add_deg_dipl = degree_level_nonvccs=="Diploma"
				gen add_deg_cert = degree_level_nonvccs=="Certificate"
				gen add_deg_aa = degree_level_nonvccs=="Associate"
				gen add_deg_bach = degree_level_nonvccs=="Bachelor"
				gen add_deg_grad = degree_level_nonvccs=="Graduate"
				gen add_deg_unk = degree_level_nonvccs=="Unknown"
				
				//Generate proper quarters for merging
					tostring strm, replace
					gen strm_quarter = ""
					replace strm_quarter = "20" + substr(strm, 2, 2) + "q" + substr(strm,4,1) if substr(strm, 1, 1)=="2"
					replace strm_quarter = "19" + substr(strm, 2, 2) + "q" + substr(strm,4,1) if substr(strm, 1, 1)=="1"
					// integer = 1 for 2005Q1, = 2 for 2005Q2, ... = 5 for 2006 Q1, etc.
					gen grad_quarter_tq2=quarterly(strm_quarter, "YQ")
					format grad_quarter_tq2 %tq
				
					drop strm_quarter strm 
				
				keep vccsid add_deg_* grad_quarter_tq2
				
				tempfile temp_nsc
				save `temp_nsc', replace
				
			//Combine VCCS and NSC grad data
				use `temp_vccs', clear
				
				append using `temp_nsc'
				
				joinby vccsid using "$project_data/01_sample_panelid.dta"
				
				//Drop out the AAS receipt of interest
				drop if grad_quarter_tq2==grad_quarter_tq & add_deg_vccs_aa==1 & string(grad_curr)==curr
				
				//Drill down to individual-term-level
				gcollapse (max) add_deg* , by(panel_id grad_quarter_tq2)
				
				foreach var of varlist add_deg* {
					replace `var'=0 if `var'==.
				}
				
				replace add_deg_dipl = 1 if add_deg_vccs_dipl==1
				replace add_deg_cert = 1 if add_deg_vccs_cert==1
				replace add_deg_aa = 1 if add_deg_vccs_aa==1
				
				drop add_deg_vccs*
				
				egen test=rowtotal(add_deg*)
				
				drop if test==0
				
				drop test
				
				rename grad_quarter_tq2 emp_quarter_tq
				
				//tsset and forward fill
				encode panel_id, gen(panel_id_num)
				tsset panel_id_num emp_quarter_tq, quarterly
				
				tsfill, full
				
				decode panel_id_num, gen(tmp)
				
				replace panel_id=tmp if panel_id==""
				
				drop tmp panel_id_num
				
				gsort panel_id emp_quarter_tq
				
				foreach var of varlist add_deg* {
				    replace `var'=0 if `var'==.
					
					by panel_id: replace `var'= `var'[_n-1] if `var'==0 & `var'[_n-1]==1
				}
				
				save "$project_data/01_degrees_over_time.dta", replace
		
		
	
	//Bring together only relevant financial aid data for demographic analysis
	
		//Minor processing of grad build file to ensure proper merging with student build files
			zipuse "$project_data/01_grad_build_aas.dta.zip", clear
			
			gen panel_id=vccsid+"_"+curr
			
			keep vccsid panel_id grad_quarter_tq acadyr
			tempfile temp1
			save `temp1', replace
			
			keep vccsid
			duplicates drop vccsid, force
			tempfile temp2
			save `temp2', replace
			
			clear
		
		local filelist: dir "$raw_data/Build/FinancialAid" files "*.dta.zip"
		local filelist: list sort filelist
		foreach file of local filelist {
			//Weird fix to properly capitalize the filenames
			//local file2 = upper(substr("`file'", 1, 1)) + substr("`file'", 2, 8) + upper(substr("`file'", 10, 1)) + substr("`file'", 11, .)
			//di "`file2'"
			preserve
				zipuse "$raw_data/Build/FinancialAid/`file'", clear
				
				//Keep only data for graduates we're looking at
					merge m:1 vccsid using `temp2', keep(match) nogenerate force
				
				save "$project_data/01_finaid_build.dta", replace
			restore
			
			append using "$project_data/01_finaid_build.dta"
		}
		
		joinby vccsid using `temp1', unmatched(using)
		
		//Keep only records prior to their graduation year, inclusive
			tostring repyear, replace
			tostring acadyr, replace
			gen repyear1=substr(repyear,1,4)
			gen repyear2=substr(repyear,5,4)
			gen acadyr2=substr(acadyr, 5,.)
			destring repyear1, replace
			destring repyear2, replace
			destring acadyr2, replace
				
			drop if acadyr2<repyear2
			drop repyear2 acadyr2
				
		
		destring pell, replace
		gen pell2 = (pell>0)
		
		
		//Pell prior to exit
		gen acadyr1 = substr(acadyr, 1, 4)
		destring acadyr1, replace
		gen acadyr2 = substr(acadyr, 5, 4)
		destring acadyr2, replace
		
		
		//Penultimate year
			gen pell_prior_exit_pen_check = 0
			replace pell_prior_exit_pen_check = 1 if repyear1==(acadyr1-1) & pell!=0
			egen pell_prior_exit_pen = max(pell_prior_exit_pen_check), by(panel_id)
		
		//Proper missingness variable for penultimate year
			//Missing if penultimate year is outside of finaid data
				gen pell_prior_exit_pen_miss_check1 = (repyear1 == acadyr1-1)
				egen pell_prior_exit_pen_miss1 = max(pell_prior_exit_pen_miss_check1), by(panel_id)
				replace pell_prior_exit_pen_miss1=1-pell_prior_exit_pen_miss1
				
			//Missing if no FAFSA filed for penultimate year
				gen pell_prior_exit_pen_miss_check2 = .
				replace pell_prior_exit_pen_miss_check2 = 1 if repyear1==(acadyr1-1) & budget==.
				replace pell_prior_exit_pen_miss_check2 = 0 if repyear1==(acadyr1-1) & budget!=.
				egen pell_prior_exit_pen_miss2 = min(pell_prior_exit_pen_miss_check2), by(panel_id)
				
			//Final variable
				gen pell_prior_exit_pen_miss = (pell_prior_exit_pen_miss1==1 | pell_prior_exit_pen_miss2==1)

		//Ultimate year
			gen pell_prior_exit_ult_check = 0
			replace pell_prior_exit_ult_check = 1 if repyear1==acadyr1 & pell!=0
			egen pell_prior_exit_ult = max(pell_prior_exit_ult_check), by(panel_id)
		
		//Proper missingness variable for ultimate year
			//Missing if ultimate year is outside of finaid data
				gen pell_prior_exit_ult_miss_check1 = (repyear1 == acadyr1)
				egen pell_prior_exit_ult_miss1 = max(pell_prior_exit_ult_miss_check1), by(panel_id)
				replace pell_prior_exit_ult_miss1=1-pell_prior_exit_ult_miss1
		
			//Missing if no FAFSA filed for penultimate year
				gen pell_prior_exit_ult_miss_check2 = .
				replace pell_prior_exit_ult_miss_check2 = 1 if repyear1==acadyr1 & budget==.
				replace pell_prior_exit_ult_miss_check2 = 0 if repyear1==acadyr1 & budget!=.
				egen pell_prior_exit_ult_miss2 = min(pell_prior_exit_ult_miss_check2), by(panel_id)
				
			//Final variable
				gen pell_prior_exit_ult_miss = (pell_prior_exit_ult_miss1==1 | pell_prior_exit_ult_miss2==1)
				
		//Collapse to student-level
			keep vccsid panel_id ///
				pell_prior_exit_pen pell_prior_exit_pen_miss ///
				pell_prior_exit_ult pell_prior_exit_ult_miss
			duplicates drop
		
		save "$project_data/01_finaid_build.dta", replace
		
		
	//Process the CIP to BGTOcc crosswalk
		import excel "$project_data/Client Version - CIP2010 to BGOT 341.xlsx", clear firstrow

		replace CIPCode=substr(CIPCode, 2, .) if substr(CIPCode,1,1)=="0"
		
		drop if CIPCode==""
		drop if BGTOccTitle=="na"
		drop if Tier==2
		drop if DegreeLevelName!="Associate's Degree"
			
		drop Tier DegreeLevel DegreeLevelName CIPTitle BGTOccTitle
			
		gduplicates drop
		
		rename CIPCode cip6
		
		save "$project_data/01_BGTCIP_crosswalk_cip6.dta", replace

		preserve
			split BGTOccCode, parse(".")
			
			drop BGTOccCode2
			rename BGTOccCode1 BGTOccCode6
			
			keep cip6 BGTOccCode6
			
			gduplicates drop
			
			save "$project_data/01_BGTCIP_crosswalk_occ6.dta", replace
		restore
			
		rename cip6 cip
			
		split cip, parse(".")
		
		replace cip2=substr(cip2, 1, 2)
		
		replace cip=cip1+"."+cip2
		
		drop cip1 cip2
			
		gduplicates drop
		
		rename cip cip4
			
		save "$project_data/01_BGTCIP_crosswalk_cip4.dta", replace
		
	//Process adjacency data for counties and MSAs from https://www.nber.org/research/data/county-adjacency
		use "$project_data/county_adjacency2010.dta", clear
		
		preserve
			import delimited using "$project_data/qcew-county-msa-csa-crosswalk-csv.csv", clear
			
			keep countycode msacode msatitle
			
			drop if msacode==""
			
			tostring countycode, replace
			rename countycode fipscounty
			
			tempfile countymsa
			save `countymsa', replace
			
			rename fipscounty fipsneighbor
			rename msacode neighbor_msacode
			rename msatitle neighbor_msatitle
			
			tempfile neighbormsa
			save `neighbormsa', replace
		restore
		
		merge m:1 fipscounty using `countymsa', nogen keep(match master)
		merge m:1 fipsneighbor using `neighbormsa', nogen keep(match master)
		
		keep msacode msatitle neighbor_msacode neighbor_msatitle
			
		drop if msacode==""
		drop if neighbor_msacode==""
		drop if msacode==neighbor_msacode
			//Dropping these would cause the neighboring MSA value to be 0 if a job is posting within the actual MSA itself
			
		gduplicates drop
		
		save "$project_data/01_msa_adjacency.dta", replace

		