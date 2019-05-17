# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import pyspark

import calendar
import datetime

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line character
    line = line.replace("\n", "")

    # 3. We split the line by tabulator characters
    params = line.split(";")

    # 4. We assign res
    if (len(params) == 7):
        res = tuple(params)

    # 5. We return res
    return res


def get_date(line):
    # 1. We create the output variable
    res = ()

    fmt = "%d-%m-%Y %H:%M:%S"
    day = calendar.day_name[(datetime.datetime.strptime(line[4], fmt)).weekday()]
    time = datetime.datetime.strptime(line[4], fmt).hour
    res = (str(day)+"_"+str(time), 1)
    return res


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, station_name):
    inputRDD = sc.textFile(my_dataset_dir)

    tupleRDD = inputRDD.map(process_line)

    filterRDD = tupleRDD.filter(lambda line: line[0] == "0" and line[5] == "0" and line[1] == station_name)
    filterRDD.persist()

    # Get Total
    shortRDD = filterRDD.map(lambda line: (line[1], 1))
    reducedRDD = shortRDD.aggregateByKey(0, lambda a, b: a + b, lambda a, b: a + b)
    totalRDD = reducedRDD.map(lambda a: a[1])
    total = totalRDD.collect()

    # Get Dates and Order
    dateRDD = filterRDD.map(get_date)
    datereduceRDD = dateRDD.aggregateByKey(0, lambda a, b: a + b, lambda a, b: a + b)
    datePercent = datereduceRDD.map(lambda line: (line[0], line[1], (float(line[1])/float(total[0]))*100))
    orderedRDD = datePercent.sortBy(lambda a: a[2], ascending=False)

    endVal = orderedRDD.collect()
    for item in endVal:
        print(item)


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input parameters as needed
    station_name = "Fitzgerald's Park"

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "D:/College/BigData/A02-4/my_dataset/*.csv"
    my_databricks_path = "/FileStore/tables/A02/my_dataset/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path
    else:
        my_dataset_dir = my_databricks_path

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir, station_name)
