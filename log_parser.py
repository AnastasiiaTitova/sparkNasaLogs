from pyspark import SparkContext
import re

your_ip = "<your ip>"
sc = SparkContext("spark://" + your_ip + ":7077", "Log Parser")
ip = your_ip + ":9000"
path = "hdfs://root@" + ip + "/nasa"
input_file = sc.textFile(path + "/log_Jul95")

code_pattern = re.compile(r'^\S+ - - \[\d{2}\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2} -\d{4}\] \"(.+)\" 5\d+ (\d+|-)\Z')
res = input_file.filter(lambda line: code_pattern.match(line))\
    .map(lambda line: (code_pattern.match(line).group(1), 1))\
    .reduceByKey(lambda a, b: a + b)
res.repartition(1).saveAsTextFile(path + "/result_task1")

timeline_pattern = re.compile(r'^\S+ - - \[(\d{2}\/\w{3}\/\d{4}):\d{2}:\d{2}:\d{2} -\d{4}\] \"(\S+) .+\" (\d+) (\d+|-)\Z')
res = input_file.filter(lambda line: timeline_pattern.match(line))\
    .map(lambda line: ((timeline_pattern.match(line).group(1, 2, 3)), 1))\
    .reduceByKey(lambda a, b: a + b)\
    .filter(lambda result: result[1] >= 10)\
    .sortBy(lambda result: result[0][0])
res.repartition(1).saveAsTextFile(path + "/result_task2")

DAYS_IN_JULY = 31
DAYS_IN_WEEK = 7
days = [i for i in range(1, DAYS_IN_JULY - DAYS_IN_WEEK + 1)]
window_pattern = re.compile(r'^\S+ - - \[(\d{2}\/\w{3}\/\d{4}):\d{2}:\d{2}:\d{2} -\d{4}\] \".+\" ([45]\d+) (\d+|-)\Z')
res = input_file.filter(lambda line: window_pattern.match(line))\
    .map(lambda line: (window_pattern.match(line).group(1), 1))\
    .reduceByKey(lambda a, b: a + b)\
    .cartesian(sc.parallelize(days))\
    .filter(lambda dates: dates[1] <= int(dates[0][0].split('/')[0]) < dates[1] + DAYS_IN_WEEK)\
    .map(lambda dates: ((dates[1], dates[1] + DAYS_IN_WEEK), dates[0][1]))\
    .reduceByKey(lambda a, b: a + b)\
    .sortBy(lambda result: result[0][0])
res.repartition(1).saveAsTextFile(path + "/result_task3")
