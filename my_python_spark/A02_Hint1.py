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

# ------------------------------------------
# FUNCTION combine_local_node
# ------------------------------------------
def combine_local_node(accum, item):
    # 1. We create the variable to return
    res = ()

    # 2. We modify the value of res
    val1 = accum[0] + item
    val2 = accum[1] + 1

    # 3. We assign res properly
    res = (val1, val2)

    # 4. We return res
    return res


# ------------------------------------------
# FUNCTION combine_different_nodes
# ------------------------------------------
def combine_different_nodes(accum1, accum2):
    # 1. We create the variable to return
    res = ()

    # 2. We modify the value of res
    val1 = accum1[0] + accum2[0]
    val2 = accum1[1] + accum2[1]

    # 3. We assign res properly
    res = (val1, val2)

    # 3. We return res
    return res


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir):
    inputRDD = sc.textFile(my_dataset_dir)

    tupleRDD = inputRDD.map(process_line)

    filterRDD = tupleRDD.filter(lambda line: line[0] == "0" and line[5] == "0")

    shortRDD = filterRDD.map(lambda line: (line[1], 1))

    reducedRDD = shortRDD.aggregateByKey(0, lambda a, b: a + b, lambda a, b: a + b)

    orderedRDD = reducedRDD.sortBy(lambda a: a[1], ascending=False)
    orderedRDD.persist()

    numPlaces = orderedRDD.count()

    endVal = orderedRDD.collect()

    print(numPlaces)
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
    # 1. Local or Databricks
    local_False_databricks_True = False

    # 2. We set the path to my_dataset and my_result
    my_local_path = "D:/College/BigData/A02-4/my_dataset/*.csv"
    my_databricks_path = "/FileStore/tables/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path
    else:
        my_dataset_dir = my_databricks_path

    # 3. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 4. We call to our main function
    my_main(sc, my_dataset_dir)
