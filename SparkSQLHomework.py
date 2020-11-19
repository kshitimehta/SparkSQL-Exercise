from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

sc = SparkSession.builder.appName("PysparkExample").config("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize","5g").config ("spark.sql.execution.arrow.enabled", "true").getOrCreate()
Student = sc.read.json('Student.json') 
Attend = sc.read.json('Attend.json')
Course = sc.read.json('Course.json')  

Student.createOrReplaceTempView("student")  
Attend.createOrReplaceTempView("attend")  
Course.createOrReplaceTempView("course")

output = sc.sql('SELECT s.student_name \
                from course as c, attend as a, student as s \
                where c.course_id = a.course_id \
                AND a.student_id = s.student_id \
                AND c.course_name = "Math"')

output.show()
output.explain()
