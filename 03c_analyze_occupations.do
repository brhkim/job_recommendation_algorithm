/**********************************************************************

	Name: 03c_analyze_occupations.do

	Purpose: Conduct analysis on earnings from BLS to create predictors
	for job quality given median salaries for each occupation at the state
	and MSA level, separately.
	
	Note this requires the externally sourced data from the BLS:
		-"$project_data/state_M2019_dl.xlsx"
		-"$project_data/MSA_M2019_dl.xlsx"
		
***********************************************************************/

	//Process state-level data
	//Downloaded from https://www.bls.gov/oes/2019/may/oes_va.htm
		import excel using "$project_data/state_M2019_dl.xlsx", clear firstrow

		keep area occ_code a_median
		
		//Keep only VA areas prior to standardization
		keep if area=="51"
		
		//Drop empty values
		drop if a_median=="*" | a_median=="#"

		//Process median salaries into standardized values
		destring a_median, replace
		gegen occ_median_all=std(a_median)
		
		//Clean up to prep for merging later
		drop a_median area
		rename occ_code SOC
		
		save "$project_data/03c_occ_median_all.dta", replace
	

	//Process MSA-level data
	//Downloaded from https://www.bls.gov/oes/2019/may/oes_13980.htm
		import excel using "$project_data/MSA_M2019_dl.xlsx", clear firstrow

		keep area_title occ_code a_median
		
		//Keep only VA areas prior to standardization
		split area_title, parse(", ")
		drop if strpos(area_title2, "VA")==0
		drop area_title1 area_title2
		
		//Drop empty values
		drop if a_median=="*" | a_median=="#"

		//Process median salaries into standardized values
		destring a_median, replace
		gegen occ_median_msa=std(a_median)
		
		//Clean up to prep for merging later
		drop a_median
		rename occ_code SOC
		rename area_title MSAName
		
		save "$project_data/03c_occ_median_msa.dta", replace
		
	
