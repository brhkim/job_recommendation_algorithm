# Using data-driven strategies to provide personalized job matches
This is the open-source code repository for our “job recommendation project,” initially developed by Brian Heseung Kim, Benjamin L. Castleman, Yifeng Song, and Alice Choe at the University of Virginia Nudge4 Solutions Lab alongside the Virginia Community College System, with generous support by the Ascendium Education Group. 

Our primary goal in this project was ultimately to improve the labor market outcomes of community college students, specifically by providing them with actionable, data-driven personalized information on available job postings as they embark on the job market immediately following graduation. More concretely, we wanted to help students filter down to available jobs that are actually relevant to them, and then support the prioritization of jobs that are mostly likely to provide them with well-paying, stable employment. More information on this project and its motivation can be found [here](https://nudge4.org/with-new-grant-researchers-aim-to-provide-community-college-graduates-with-personalized-job-matches/), while the final project report can be found [here](https://brhkim.com/wp-content/uploads/Final-Ascendium-Report-BK.pdf).

The codebase here represents our working “beta” product, and was produced to explore the feasibility and value of such an algorithm in concept prior to testing its utility directly in a larger-scale field experiment with our community college system partners. We have opted not to continue with a pilot test, but provide this codebase to the public so that others can learn from and build upon our experiences developing the algorithm to date. 

As the codebase relies on private student-level data from the Virginia Community College System that cannot be provided publicly, our code is useful for instructional and illustrative purposes only (i.e., full replication using the code “out of the box” is not possible outside this context). Even so, we have provided ample in-line documentation in each script to explain the necessary components, describe the flow of analyses, and highlight where adjustments would be beneficial for others attempting to explore this work for their own ends.

To summarize the workflow of this project, we proceed in four main steps (where each step is numbered in accordance with the relevant script):
1. Compile and organize data from several sources as needed for the job recommendation algorithm, including:
    * a. Student-level academic and demographic data from our community college partners
    * b. Historic employment and earnings data of all community college students, also from our community college partners
    * c. Job postings data purchased from Burning Glass
    * d. Occupation-specific average earnings data at the state- and county-level from the Bureau of Labor Statistics (BLS)
2. Clean, consolidate, and restructure the aforementioned data for analysis
3. Analyze historical employment and job listings data to generate benchmarks of job relevance and quality based on employer data and other observable job data from posted listings (e.g., occupation)
4. Produce a set of job listings, ranked by their relevance and quality, personalized to each student in a provided sample of interest.

Our general gameplan for the algorithm is to take the universe of contemporaneous job postings from the Burning Glass data, identify for each student which jobs are relevant to them both academically and geographically using the student-level data, and finally sort those jobs based on the best proxies for job quality available to us from the various data sources described above. Within each script, you will also find more specific explanation, documentation, and suggestions for adapting the code to your own contexts as useful. 

More information about some of the design decisions and challenges of this algorithm can be found in the blog post we released [here](https://nudge4.org/fall-2021-update-using-data-driven-strategies-to-provide-personalized-job-matches/). You can also find the final report that updates these reflections, describes our assessments of the algorithm’s efficacy, and discusses some of the challenges we forecasted in terms of actual implementation [here](https://brhkim.com/wp-content/uploads/Final-Ascendium-Report-BK.pdf).

Again, we provide this codebase as-is, but welcome questions, concerns, suggestions, and other engagement from interested researchers and developers seeking to learn from or build upon our work thus far – please don’t hesitate to reach out to Brian Heseung Kim at brhkim@gmail.com.
