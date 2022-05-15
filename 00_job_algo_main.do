/**********************************************************************

	Name: 00_job_algo_main.do
	Created: February, 2021

	Purpose: This main do-file runs through all of the requisite analytic
	steps to produce job quality and relevance indices for a list of
	Burning Glass job postings. The final script (04_recommendation_analysis) 
	then takes these analytic files and a list of available jobs and provides, 
	for a single student's data, a list of jobs with relevance and quality
	rankings.
	
	This set of scripts is intended more to be a detailed schematic than
	a drop-in-place solution, given that data structures and availability
	will undoubtedly vary by context. When possible, we provide
	detailed descriptions of our own data structures so that you can
	mirror these data structures in your own context and adapt this code
	accordingly.
	
	We also include the following externally-sourced datafiles alongside
	this project that are required for several steps of analysis:
		-"$project_data/county_adjacency2010.dta"
		-"$project_data/Client Version - CIP2010 to BGOT 341.xlsx"
		-"$project_data/qcew-county-msa-csa-crosswalk-csv.csv"
		-"$project_data/state_M2019_dl.xlsx"
		-"$project_data/MSA_M2019_dl.xlsx"
		
	This script should be run only after the Burning Glass data cleaning steps
	provided separately, as well as the Employment UI - Burning Glass employer
	name matching script step, also provided separately.
	
		
***********************************************************************/

/***********************************************************************

	#setup - Setup file and prepare directory global

***********************************************************************/

	//Basics
		clear all
		set more off
		set matsize 2000
		version 16
	
	//Directory globals
		global username="`c(username)'"
		global root "/Users/${username}/Box Sync/Job Recommendation Algorithm Development"
		global raw_data "/Users/${username}/Box Sync/VCCS restricted student data"
			//Where your student-level data is located
		
		global project_data "$root/data"
			//Intermediary data file location; also includes externally-sourced data files, e.g., from BLS
		
		global project_output "$root/output"
			//Output folder
		
		global scripts "/Users/${username}/Box Sync/GitHub/cc_job_recommendations"
			//Folder where scripts live
		
		global bg_data "/Users/${username}/Box Sync/BG_DATA"
			//Folder where Burning Glass jobs data live; see Burning Glass data processing file for more information on how this should be formatted

		cd "$root"
	
	//Set logging file
		global sysdate=c(current_date)
		global sysdate=subinstr("$sysdate", " ", "", .)
		capture log close
		log using "${scripts}/logs/job_algo_log_$sysdate.log", replace
	
	//Sample of interest
		global cohorts 20002001 20012002 20022003 20032004 20042005 20052006 20062007 20072008 20082009 20092010 20102011 20112012 20122013 20132014 20142015 20152016 20162017 20172018
			//We use this to set which graduation cohort datafiles we incorporate into the employer quality index calculations.
			//Cohorts is aligned to the graduation file nomenclature, and thus reflect the year students *graduated*, not enrolled.

	//Analysis adjustments
		global firstyear 2011
		global lastyear 2019
			//We set these globals to read in the corresponding data files for employment UI data and for NSC re-enrollment data
			//Use only complete years for ease
		
		global job_post_window = 2
			//How many quarters before and after a student's graduation quarter a job must be posted by to consider at all
			//e.g. a value of 1 means we'll show the jobs posted one quarter before and after a student's graduation,
			//in addition to the jobs posted during
			
	//Package dependencies
		//capture ssc install reghdfe
		//capture ssc install zipsave
		//capture ssc install gtools
		//capture ssc install ftools
		//capture ssc install tsspell
		//capture ssc install labutil
		//capture ssc install geodist

	//Local run-switches for analytic code
		local switch_compile = 0
		local switch_clean = 0
		local switch_analyze_employers = 0
		local switch_analyze_jobs = 0
		local switch_analyze_occupations = 0
		local switch_recommendation_analysis = 0
	
/***********************************************************************

	#run - Run various component do-files according to run-switches set above

***********************************************************************/

//See each individual file's header text for details on what each does

if `switch_compile' == 1 {
	do "${scripts}/01_compile_sample_data.do"
}

if `switch_clean' == 1 {
	do "${scripts}/02_clean_sample_data.do"
}

if `switch_analyze_employers' == 1 {
	do "${scripts}/03a_analyze_employers.do"
}

if `switch_analyze_jobs' == 1 {
	do "${scripts}/03b_analyze_jobs.do"
}

if `switch_analyze_occupations' == 1 {
	do "${scripts}/03c_analyze_occupations.do"
}

if `switch_recommendation_analysis' == 1 {
	do "${scripts}/04_recommendation_analysis.do"
}

capture log close
