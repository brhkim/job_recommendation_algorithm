/**********************************************************************

	Name: 03b_analyze_jobs.do

	Purpose: Conduct analysis on Burning Glass jobs to estimate the average
	listed salary of jobs with the same occupation code across Virginia and
	across each MSA. We also calculate the average listed salary of each job*employer
	combination across Virginia and across each MSA.
		
***********************************************************************/

	//Get jobs list
		clear
		local filelist: dir "$bg_data/Main" files "*.dta.zip"
		local filelist: list sort filelist
		foreach file of local filelist {
			di "`file'"
			preserve
				zipuse "$bg_data/Main/`file'", clear
				
				drop JobId
				
				//Pare down dataset to only those we would think are relevant to students
				drop if Internship==1
				drop if TaxTerm=="contractor"
				drop if JobHours=="parttime"
				drop if Degree=="Bachelor's" | Degree=="Master's" | Degree=="PhD"
				drop if MaxDegree=="High School"
				drop if Employer=="na" | BGTOcc=="na"
				drop if (MinSalary>0 & MinSalary<27300) | (MaxSalary>0 & MaxSalary<27300)
				replace MinSalary=. if MinSalary==-999
				replace MaxSalary=. if MaxSalary==-999
				drop if missing(MinSalary) & missing(MaxSalary)

				tempfile maintmp
				save `maintmp', replace
			restore
			
			append using `maintmp'
		}
		
	//Start data cleaning
		rename BGTOcc BGTOccCode
		
		gen MidSalary=( MaxSalary + MinSalary)/2
		
		gen count=1
		
	//Get down to the occupation-level across state
		preserve
			gcollapse (mean) MidSalary (sum) count, by(BGTOccCode)
		
			gegen salary_avg_all=std(MidSalary)
			
			drop if count<10
			
			drop count
			
			save "$project_data/03b_jobs_avg_all.dta", replace
		restore
		
	//Get down to the occupation-level across MSAs
		preserve
			gcollapse (mean) MidSalary (sum) count, by(BGTOccCode MSAName)
		
			gegen salary_avg_msa=std(MidSalary)
			
			drop if count<5
			
			drop count
			
			save "$project_data/03b_jobs_avg_msa.dta", replace
		restore

	//Get down to the occupation-employer-level across state
		preserve
			gcollapse (mean) MidSalary (sum) count, by(BGTOccCode Employer)
		
			gegen salary_emp_all=std(MidSalary)

			drop if count<2
			
			drop count
			
			save "$project_data/03b_jobs_emp_all.dta", replace
		restore
		
	//Get down to the occupation-employer-level across MSAs
		preserve
			gcollapse (mean) MidSalary (sum) count, by(BGTOccCode MSAName Employer)
		
			gegen salary_emp_msa=std(MidSalary)
			
			drop if count<2
			
			drop count
			
			save "$project_data/03b_jobs_emp_msa.dta", replace
		restore



	
