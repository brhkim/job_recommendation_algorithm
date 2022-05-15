/**********************************************************************

	Name: 04_recommendation_analysis.do

	Purpose: Take a provided list of students, then loop through them and 
	create a dataset of ranked jobs for each of them.
	
	This code should be run only after all prior scripts to produce the
	relevant data files used for the job ranking calculations.
	
	We provide a datafile, "$project_data/04_students_list_example.dta",
	filled with dummy data alongside this codebase to show the structure
	necessary for this script to be run correctly. Variables necessary are:
		vccsid college curr curr_text cip6 cip4 grad_quarter_tq 
		home_county home_lat home_lon home_msa_code home_msa_title 
		college_county college_lat college_lon college_msa_code college_msa_title
		
	Ultimately, this example list would be replaced by a list of near-graduates
	formatted the same way so that this script would loop through each of the
	near-graduates and produce a datafile of jobs for that student that would
	then be restructured and fed into the user-facing system.
	
	Note that the Burning Glass jobs data should be the most updated jobs
	available per their add/delete live feed instructions, rather than
	historical data as loaded here.
	
		
***********************************************************************/
	
	//Load up the example student list dataset
	use "$project_data/04_students_list_example.dta", clear
	
	/* BK used this code to generate a test run of the algorithm output across a specific set of students instead of using the example list
	zipuse "$project_data/02_employer_quality_clean.dta.zip", clear
	
	//Only need these variables
	keep vccsid college curr curr_text cip6 grad_quarter_tq home_county home_lat home_lon college_county college_lat college_lon home_msa_code home_msa_title college_msa_code college_msa_title cip4 pell_prior_exit gender new_race first_gen
	
	//Restrict to a specific quarter of graduates
	keep if grad_quarter_tq==tq(2017q2)
	duplicates drop
	
	//Restrict to a couple colleges just to test first
	keep if college=="Wytheville" | college=="Tidewater" | college=="Piedmont Virginia"
	
	*/
	
	save "$project_data/04_recommendation_case_all.dta", replace
	
	//Create an individual data file for each student in the list
	local N=_N
	forvalues i=1/`N' {
		preserve
			keep if _n==`i'
			
			gen merger=1
			
			save "$project_data/04_recommendation_case_`i'.dta", replace
		restore
	}

	//Loop through constructing job recommendations for each student
	forvalues i=1/`N' {
		//local i = 7
		
		use "$project_data/04_recommendation_case_`i'.dta", clear
		
		//Get their graduating quarter
		gen year=yofd(dofq(grad_quarter_tq))
		
		sum year, detail
		local year_local=r(max)
		
		//Get the dataset number for this individual student
		local seedval = `i'
		
	//Find relevant jobs at each "level" of match with their CIP
		//First using the BGT crosswalk to identify relevant occupation codes
			use "$project_data/01_BGTCIP_crosswalk_cip6.dta", clear
				
			merge m:1 cip6 using "$project_data/04_recommendation_case_`seedval'.dta", keep(match)
			keep BGTOccCode
			gen cip6_match=1
			
			capture duplicates drop
				
			tempfile cip6
			save `cip6', replace
				
			use "$project_data/01_BGTCIP_crosswalk_cip4.dta", clear
				
			merge m:1 cip4 using "$project_data/04_recommendation_case_`seedval'.dta", keep(match)
			keep BGTOccCode
			gen cip4_match=1
			
			capture duplicates drop
				
			tempfile cip4
			save `cip4', replace
			
			use "$project_data/01_BGTCIP_crosswalk_occ6.dta", clear
				
			merge m:1 cip6 using "$project_data/04_recommendation_case_`seedval'.dta", keep(match)
			keep BGTOccCode6
			gen occ6_match=1
			
			capture duplicates drop
				
			tempfile occ6
			save `occ6', replace
		

		//Now using job-specific CIP listings from the Burning Glass data
			local start_year=`year_local'-1
			local end_year=`year_local'+1

			forvalues year_data=`start_year'/`end_year' {
				forvalues quarter_data=1/4 {
					if `year_data'==`start_year' & `quarter_data'==1 {
						zipuse "$bg_data/CIP/cip_`year_data'-q`quarter_data'.dta.zip", clear
					}
					else {
						zipappend using "$bg_data/CIP/cip_`year_data'-q`quarter_data'.dta.zip"
					}
				}
			}
		
			//Clean up CIP codes first
				keep BGTJobId CIP
			
				rename CIP cip
				tostring cip, replace
				replace cip=cip+".0000" if strpos(cip, ".")==0
				split cip, parse(".")
				gen cip2_len=strlen(cip2)
				
				replace cip=cip+"0" if cip2_len==3
				replace cip=cip+"00" if cip2_len==2
				replace cip=cip+"000" if cip2_len==1
				
				drop cip1 cip2 cip2_len
				
				split cip, parse(".")
				
				rename cip cip6
				
				replace cip2=substr(cip2, 1, 2)
				
				gen cip4=cip1+"."+cip2
				
				drop cip1 cip2
				
			//Now merge in student CIP
				merge m:1 cip6 using "$project_data/04_recommendation_case_`seedval'.dta", gen(cip6_merge)
				
				merge m:1 cip4 using "$project_data/04_recommendation_case_`seedval'.dta", gen(cip4_merge)
				
				gen cip6_match=1 if cip6_merge==3
				gen cip4_match=1 if cip4_merge==3
				
				keep if cip6_match==1 | cip4_match==1
				
				collapse (max) cip6_match cip4_match, by(BGTJobId)
				
				tempfile jobmatches
				save `jobmatches', replace
			

	//Get jobs from Burning Glass data
		local start_year=`year_local'-1
		local end_year=`year_local'+1

		forvalues year_data=`start_year'/`end_year' {
			forvalues quarter_data=1/4 {
				if `year_data'==`start_year' & `quarter_data'==1 {
					zipuse "$bg_data/Main/main_`year_data'-q`quarter_data'.dta.zip", clear
				}
				else {
					zipappend using "$bg_data/Main/main_`year_data'-q`quarter_data'.dta.zip"
				}
			}
		}
				
		drop if Internship==1
		drop if TaxTerm=="contractor"
		drop if JobHours=="parttime"
		drop if Degree=="Bachelor's" | Degree=="Master's" | Degree=="PhD"
		drop if MaxDegree=="High School"
		drop if Employer=="na" | BGTOcc=="na"
		
		//Bring in student data
		gen merger=1
		merge m:1 merger using "$project_data/04_recommendation_case_`seedval'.dta", nogen
		
		//Begin generating actual predictor variables
		
		//CIP-Occupation matching
			rename BGTOcc BGTOccCode
			split BGTOccCode, parse(".")
			rename BGTOccCode1 BGTOccCode6
			drop BGTOccCode2
		
			merge m:1 BGTOccCode using `cip6', gen(cip6_occmerge) update keep(match match_update master)
			merge m:1 BGTOccCode using `cip4', gen(cip4_occmerge) update keep(match match_update master)
			merge m:1 BGTOccCode6 using `occ6', gen(cip6_occ6merge) update keep(match match_update master)
			merge m:1 BGTJobId using `jobmatches', gen(cip_jobmerge) update keep(match match_update master)
			
			replace cip6_match=0 if missing(cip6_match)
			replace cip4_match=0 if missing(cip4_match)
			replace occ6_match=0 if missing(occ6_match)
			
			replace occ6_match=0 if cip6_match==1
			replace cip4_match=0 if cip6_match==1
			
			drop if cip6_match==0 & cip4_match==0 & occ6_match==0

		//Job Post Timing
			//Process dates into quarters
			gen post_year=substr(source_date, 1, 4)
			gen post_month=substr(source_date, 6, 2)
		
			gen qtr=""
			replace qtr="1" if inlist(post_month, "01", "02", "03")
			replace qtr="2" if inlist(post_month, "04", "05", "06")
			replace qtr="3" if inlist(post_month, "07", "08", "09")
			replace qtr="4" if inlist(post_month, "10", "11", "12")
			
			gen post_qtr_prep = post_year+"q"+qtr
		
			gen post_quarter_tq=quarterly(post_qtr_prep, "YQ")
			format post_quarter_tq %tq
			
			gen posted_same_quarter=(grad_quarter_tq==post_quarter_tq)
			gen posted_1prior_quarter=(grad_quarter_tq-1==post_quarter_tq)
			gen posted_2prior_quarter=(grad_quarter_tq-2==post_quarter_tq)
			gen posted_1after_quarter=(grad_quarter_tq+1==post_quarter_tq)
			gen posted_2after_quarter=(grad_quarter_tq+2==post_quarter_tq)
			
			drop if posted_same_quarter==0 & posted_1prior_quarter==0 & posted_2prior_quarter==0 & ///
				posted_1after_quarter==0 & posted_2after_quarter==0
		
		//Listed job location: actual
			gen geo_home_county_match=(home_county==FIPS)
			gen geo_college_county_match=(college_county==FIPS)
			
			gen geo_home_msa_match=(home_msa_title==MSAName)
			gen geo_college_msa_match=(college_msa_title==MSAName)

			replace Lat=. if Lat==-999
			replace Lon=. if Lon==-999
			geodist Lat Lon home_lat home_lon, generate(geo_home_dist) miles
			geodist Lat Lon college_lat college_lon, generate(geo_college_dist) miles
		
		//Listed job location: adjacency
			preserve
				use "$project_data/county_adjacency2010.dta", clear
				
				rename fipscounty home_county
				
				drop if home_county==fipsneighbor
				
				merge m:1 home_county using "$project_data/04_recommendation_case_`seedval'.dta", nogen keep(match)
				
				keep fipsneighbor
				
				rename fipsneighbor FIPS
				
				tempfile homeneighborcounty
				save `homeneighborcounty'
			restore
			
			merge m:1 FIPS using `homeneighborcounty', gen(homeneighborcounty_merge) keep(match master)
			
			gen geo_home_county_neighbor=(homeneighborcounty_merge==3)
			
			preserve
				use "$project_data/county_adjacency2010.dta", clear
				
				rename fipscounty college_county
				
				drop if college_county==fipsneighbor
				
				merge m:1 college_county using "$project_data/04_recommendation_case_`seedval'.dta", nogen keep(match)
				
				keep fipsneighbor
				
				rename fipsneighbor FIPS
				
				tempfile collegeneighborcounty
				save `collegeneighborcounty'
			restore
			
			merge m:1 FIPS using `collegeneighborcounty', gen(collegeneighborcounty_merge) keep(match master)
			
			gen geo_college_county_neighbor=(collegeneighborcounty_merge==3)
			
			preserve
				use "$project_data/01_msa_adjacency.dta", clear
				
				rename msatitle home_msa_title
				
				merge m:1 home_msa_title using "$project_data/04_recommendation_case_`seedval'.dta", nogen keep(match)
				
				keep neighbor_msatitle
				
				rename neighbor_msatitle MSAName
				
				tempfile homeneighbormsa
				save `homeneighbormsa'
			restore
		
			merge m:1 MSAName using `homeneighbormsa', gen(homeneighbormsa_merge) keep(match master)
		
			gen geo_home_msa_neighbor=(homeneighbormsa_merge==3)
		
			preserve
				use "$project_data/01_msa_adjacency.dta", clear
				
				rename msatitle college_msa_title
				
				merge m:1 college_msa_title using "$project_data/04_recommendation_case_`seedval'.dta", nogen keep(match)
				
				keep neighbor_msatitle
				
				rename neighbor_msatitle MSAName
				
				tempfile collegeneighbormsa
				save `collegeneighbormsa'
			restore
		
			merge m:1 MSAName using `collegeneighbormsa', gen(collegeneighbormsa_merge) keep(match master)
		
			gen geo_college_msa_neighbor=(collegeneighbormsa_merge==3)
			
			drop if geo_home_county_match==0 & geo_college_county_match==0 & geo_home_msa_match==0 & ///
				geo_college_msa_match==0 & geo_home_county_neighbor==0 & geo_college_county_neighbor==0 & ///
				geo_home_msa_neighbor==0 & geo_college_msa_neighbor==0
			
		//Cull down to analytic dataset
		keep vccsid BGTJobId CleanTitle BGTOccName BGTOccCode Employer City County MSAName MinSalary MaxSalary SOC ///
			grad_quarter_tq college curr curr_text home_county home_msa_title college_county  college_msa_title ///
			post_quarter_tq posted_same_quarter posted_1prior_quarter posted_2prior_quarter posted_1after_quarter posted_2after_quarter ///
			cip6_match cip4_match occ6_match ///
			geo_*
		
		//Standardize variables
			sum geo_home_dist, detail
			local home_dist_min=r(min)
			local home_dist_max=r(max)

			replace geo_home_dist = 1-((geo_home_dist-`home_dist_min')/(`home_dist_max' - `home_dist_min'))
			
			sum geo_college_dist, detail
			local college_dist_min=r(min)
			local college_dist_max=r(max)

			replace geo_college_dist = 1-((geo_college_dist-`college_dist_min')/(`college_dist_max' - `college_dist_min'))
		
		//Process weights
		preserve 
			import excel using "$root/docs/Ensemble Predictor Ratings/Fine Ratings/predictor_fine_ranking_combined.xlsx", sheet("Job Relevance Rescaled") clear firstrow
			
			drop if Varname==""
			
			keep Varname Average
			
			count
			local totalrows=r(N)
			
			tempfile weights
			save `weights', replace
		restore
		
		forvalues row=1/`totalrows' {
			preserve
				use `weights', clear
			
				local namecheck=Varname[`row']
				local weightcheck=Average[`row']
			restore
			
			gen r_`namecheck'=`namecheck' * `weightcheck'
		}
		
		gegen relevance_index=rowtotal(r_*)
		
		compress
		
		save "$project_data/04_recommendation_case_prep_`seedval'.dta", replace
		
	//Start calculating and cleaning to get job quality indices prepared
	
		//local seedval=3
		use "$project_data/04_recommendation_case_prep_`seedval'.dta", clear
		
		//First, listed salary variables
			replace MinSalary=. if MinSalary==-999
			replace MaxSalary=. if MaxSalary==-999
			
			gen MidSalary=( MaxSalary + MinSalary)/2
			
			gegen MidSalary_std=std(MidSalary)
		
			//Merge in listed salaries of other jobs with same occ codes: across VA
				merge m:1 BGTOccCode using "$project_data/03b_jobs_avg_all.dta", keep(match master) nogen
	
			//Merge in listed salaries of other jobs with same occ codes: across MSAs
				merge m:1 BGTOccCode MSAName using "$project_data/03b_jobs_avg_msa.dta", keep(match master) nogen
	
			//Merge in listed salaries of other jobs with same occ codes *from same employer*: across VA
				merge m:1 BGTOccCode Employer using "$project_data/03b_jobs_emp_all.dta", keep(match master) nogen
				
			//Merge in listed salaries of other jobs with same occ codes *from same employer*: across MSAs
				merge m:1 BGTOccCode MSAName Employer using "$project_data/03b_jobs_emp_msa.dta", keep(match master) nogen
	
		//Next, move onto historical occupation earnings data from BLS
			merge m:1 SOC using "$project_data/03c_occ_median_all.dta", keep(match master) nogen
			merge m:1 SOC MSAName using "$project_data/03c_occ_median_msa.dta", keep(match master) nogen
	
		//Finally, employer quality indices
			preserve
				import excel "$project_data/employer_name_crosswalk_v1.xlsx", firstrow clear
				rename employer_name_bg Employer
				rename employer_name_vccs_clean employer_name
				
				keep Employer employer_name
				
				gduplicates drop
				
				tempfile name_merge
				save `name_merge', replace
			restore
			
			replace Employer=upper(Employer)
			
			merge m:1 Employer using `name_merge', keep(match master) nogen
			
			merge m:1 employer_name using "$project_data/03a_fe_estimates_all.dta", keep(match master) nogen
			merge m:1 employer_name college using "$project_data/03a_fe_estimates_coll.dta", keep(match master) nogen
			merge m:1 employer_name curr using "$project_data/03a_fe_estimates_curr.dta", keep(match master) nogen
			merge m:1 employer_name college curr using "$project_data/03a_fe_estimates_collcurr.dta", keep(match master) nogen
	
	//Now, clean up empty/missing quality variables
		foreach var of varlist MidSalary_std salary_avg_all salary_avg_msa salary_emp_all salary_emp_msa occ_median_all occ_median_msa w_fe_all lwe_fe_all emp_8q_fe_all w_fe_coll lwe_fe_coll emp_8q_fe_coll w_fe_curr lwe_fe_curr emp_8q_fe_curr w_fe_collcurr lwe_fe_collcurr emp_8q_fe_collcurr {
		    replace `var'=0 if missing(`var')
		}
		
		
	//Scale everything by the determined weights
		preserve 
			import excel using "$root/docs/Ensemble Predictor Ratings/Fine Ratings/predictor_fine_ranking_combined.xlsx", sheet("Job Quality Rescaled") clear firstrow
			
			drop if Varname==""
			
			keep Varname Average
			
			count
			local totalrows=r(N)
			
			tempfile weights
			save `weights', replace
		restore
		
		forvalues row=1/`totalrows' {
			preserve
				use `weights', clear
			
				local namecheck=Varname[`row']
				local weightcheck=Average[`row']
			restore
			
			gen q_`namecheck'=`namecheck' * `weightcheck'
		}
		
		gegen quality_index=rowtotal(q_*)
		
		compress
		
	//Make a scaled joint index!
		sum relevance_index, detail
		local relevance_index_min=r(min)
		local relevance_index_max=r(max)

		gen relevance_index_scaled = (relevance_index-`relevance_index_min')/(`relevance_index_max' - `relevance_index_min')
		
		sum quality_index, detail
		local quality_index_min=r(min)
		local quality_index_max=r(max)

		gen quality_index_scaled = (quality_index-`quality_index_min')/(`quality_index_max' - `quality_index_min')
		
		replace relevance_index_scaled=0.00001 if relevance_index_scaled==0 | relevance_index_scaled<0.00001
		replace quality_index_scaled=0.00001 if quality_index_scaled==0 | quality_index_scaled<0.00001
		replace relevance_index_scaled=1 if relevance_index_scaled>1 & !missing(relevance_index_scaled)
		replace quality_index_scaled=1 if quality_index_scaled>1 & !missing(quality_index_scaled)
		
		gen joint_index=2/((1/relevance_index_scaled)+(1/quality_index_scaled))
		
		sort joint_index
		
		save "$project_data/04_recommendation_case_output_`seedval'.dta", replace
		
		export delimited "$project_data/04_recommendation_case_output_`seedval'.csv", replace
	
	}
	
	//Now that we've produced job matches for each student, compile all of them
	//together into a single data file that can be merged back to student data
	//using student id's (in this case, vccsid)
	use "$project_data/04_recommendation_case_output_1.dta", clear
	
	//local N = 5
	
	forvalues i=2/`N' {
		append using "$project_data/04_recommendation_case_output_`i'.dta"
	}
	
	//Save it
	save "$project_data/04_recommendation_case_output_combined.dta", replace
	
