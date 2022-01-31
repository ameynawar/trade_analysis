# trade_analysis


Problem statement:
Given a sales data of retail chain, we need to find historical sales to build trends over defined time intervals over Department or Customer or both.
We have to build this trend for last 30 days from max date in the input table.
Definitions:
Example Dataset: Consider below sample sales data set:
Table SALES:

SALES_DATE |	SALES_DEP |	SALES_ITEM |	SALES_CONSUMER |	SALES_QUANTITY 
------ | ------ |  ------ | ------ | ------ 
12-11-2021 |	FASHION	 |CLOTHES	|Nilesh | 5
11-11-2021 |	ELECTRONICS|	TV	|Nilesh	 | 1
10-11-2021	|MEDICAL	|COUGH SYRUP	|Nilesh |	1
09-11-2021	|GROCERY	|BISCUITS	|Nilesh |	2

Output TREND:

SALES_DATE |	SALES_DEP |	SALES_7DAY_DATE	|SALES_TODAY_QUANTITY |	SALES_7DAY_QUANTITY
------ | ------ |  ------ | ------ | ------
12-11-2021	|FASHION |	05-11-2021	|5	|2.00

#How to execute this program,
Please import this project as Maven project.
Please execute pom.xml which will get all the required dependencies.

This program takes two arguments.
First is Number of days for we want to generate the report. 
SECOND is Column name for which we want to generate report. 
For eg.
    TradeAnalysis.java "7" "SALES_DEP"

#solution
To get historical sales trend, We have using window and lags these two analytical function,
which are provided in Spark. 

#Note
When i was Solving this problem. I noticed the value 2 for column SALES_7DAY_QUANTITY in output Dataset.
Is missing in the Input. If This values has to calculated using Linear regression then This dataset is not enough and 
Output will be different based on the parameters. So i have added One extra row in the input. 
This is the dataset I have used to solve the problem.

SALES_DATE |	SALES_DEP |	SALES_ITEM |	SALES_CONSUMER |	SALES_QUANTITY
------ | ------ |  ------ | ------ | ------ 
12-11-2021 |	FASHION	 |CLOTHES	|Nilesh | 5
11-11-2021 |	ELECTRONICS|	TV	|Nilesh	 | 1
10-11-2021	|MEDICAL	|COUGH SYRUP	|Nilesh |	1
09-11-2021	|GROCERY	|BISCUITS	|Nilesh |	2
05-11-2021 |FASHION|CLOTHES|NILESH|2
