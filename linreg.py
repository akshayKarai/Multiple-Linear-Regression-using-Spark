#Akshay Karai 801038933

# linreg.py # 
# Standalone Python/Spark program to perform linear regression. 
# Performs linear regression by computing the summation form of the 
# closed form expression for the ordinary least squares estimate of beta. 
# # TODO: We caluclate the ordinary least squared estimate of beta with simple equation. # 
# We compute this using the help of numpy package
# Takes the yx file as input, where on each line y is the first element 
# and the remaining elements constitute the x. 
# # Usage: spark-submit linreg.py <inputdatafile> 
# Example usage: spark-submit linreg.py yxlin.csv # 


import sys
import numpy as np    # using numpy for array and matrices and arithmetic expressions on it

from numpy.linalg import inv  # Package for doing inverse
from pyspark import SparkContext  #defining the spark context

# This method calculates XT*X
# asmatrix interprets the input as a matrix
# np.dot Dot products two arrays
def part_one(x_mat):
    matrix_one = np.asmatrix(x_mat).T  # XT part
    matrix_two = np.asmatrix(x_mat)   # X part
    matrix_result = np.dot(matrix_one,matrix_two)    
    return matrix_result

# This method calculates XT.Y
def part_two(xy_mat):
    matrix_one = np.asmatrix(xy_mat[1:]).T   # XT part
    matrix_two = np.asmatrix(xy_mat[0])      # Y part
    matrix_result = np.dot(matrix_one,matrix_two)
    return matrix_result

# Main code start from here
if __name__ == "__main__":
  if len(sys.argv) !=2:
    print >> sys.stderr, "Usage: linreg <datafile>"
    # if the input is not in improper format, display error and exit
    exit(-1)

  sc = SparkContext(appName="LinearRegression")  #Definining spark context

  yxinputFile = sc.textFile(sys.argv[1])  #Taking the input path from command line argument
  
  yxlines = yxinputFile.map(lambda line: line.split(','))   #Split the csv file and store it in RDD

  X = np.array(yxlines.map(lambda value: value[1:]).collect()).astype('float')
  
  Y = np.array(yxlines.map(lambda value: value[0]).collect()).astype('float') 

  # map reads from RDD, applies transformation and creates a new RDD object
  
  array_ones = np.ones(len(X))  # creating an array having 1's with length equal to x

  x_bias = np.c_[array_ones,X] # concatenating this array with the X which forms n*2 matrix with 1 in the first part
  
  y_bias = np.c_[Y,x_bias]  #concatenating the above file and y
  
  # as per doc parallelize will create a new RDD object
  # in below result_A and result_B takes each element from the array
  # applies X.XT and XT.Y respectively
  # Reducebykey sums all the elements from this derived matrix after transformation using the key value pairs

  result_A = sc.parallelize(x_bias).map(lambda a: ("Key1",part_one(a))).reduceByKey(lambda x,y : x+y).values()

  result_B = sc.parallelize(y_bias).map(lambda b:("Key2",part_two(b))).reduceByKey(lambda x,y : x+y).values()
  
  # collect will get all the objects together
  # squeeze function removes one-dimensional entry from the shape of the given array.

  A = np.array(result_A.collect()).squeeze()

  B = np.array(result_B.collect())
  
  # inverse function is used to inverse A
  A_inverse = inv(A)
  
  # beta is calculated by multiplying the part one and part two of the equation

  beta = np.dot(A_inverse,B)
  
  print "beta: "
  for coeff in beta:
      print coeff

  sc.stop()
