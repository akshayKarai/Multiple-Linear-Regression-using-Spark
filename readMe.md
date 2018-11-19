Assignment 4

Name: Akshay karai
Email: akarai@uncc.edu

Main file is linreg.py
_____________________________________________________________________

Step 1: Copy the python file and input files into dsba-cluster

scp linreg <username>@dsba-hadoop.uncc.edu:/users/<username> 

scp yxlin.csv <username>@dsba-hadoop.uncc.edu:/users/<username> 

_____________________________________________________________________
			
Step 2: Copy the python file and input files into hadoop filesystem

hadoop fs -put yxlin.csv /user/<username>/

_____________________________________________________________________
			
Step 3: Execute the code using spark-submit 

 spark-submit linreg.py <inputdatafile>
_____________________________________________________________________


Output will printed in the console. Beta values will be printed

Output files are also included ( xylin.out , xylin2.out )



