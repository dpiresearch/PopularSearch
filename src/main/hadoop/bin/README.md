calcSearchAdKwCount.sh

=====================

This script runs the pig program (searchViewCount.pig) that matches search keywords with page views.  It's kicked off by Azkaban as part of a workflow.  

Before running pig, it uses the days_ago and days_back parameters to calculate the dates that are involved in the calculation.