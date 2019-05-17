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
from bsddb import dbutils

import pyspark
import pyspark.streaming

import os
import shutil
import time

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
# FUNCTION my_model
# ------------------------------------------
def my_model(ssc, monitoring_dir, result_dir, time_step_interval, window_duration, sliding_duration):
    inputDStream = ssc.textFileStream(monitoring_dir)

    tupleDStream = inputDStream.map(process_line)

    filterDStream = tupleDStream.filter(lambda line: line[0] == "0" and line[5] == "0")

    shortDStream = filterDStream.map(lambda line: (line[1], 1))

    reducedDStream = shortDStream.reduceByKey(lambda a, b: a + b)

    orderedDStream = reducedDStream.transform(lambda newRDD: newRDD.sortBy(lambda a: a[1], ascending=False))

    orderedDStream.pprint()

# ------------------------------------------
# FUNCTION create_ssc
# ------------------------------------------
def create_ssc(sc, monitoring_dir, result_dir, max_micro_batches, time_step_interval, window_duration, sliding_duration):
    # 1. We create the new Spark Streaming context.
    # This is the main entry point for streaming functionality. It requires two parameters:
    # (*) The underlying SparkContext that it will use to process the data.
    # (**) A batch interval, specifying how often it will check for the arrival of new data,
    # so as to process it.
    ssc = pyspark.streaming.StreamingContext(sc, time_step_interval)

    # 2. We configure the maximum amount of time the data is retained.
    # Think of it: If you have a SparkStreaming operating 24/7, the amount of data it is processing will
    # only grow. This is simply unaffordable!
    # Thus, this parameter sets maximum time duration past arrived data is still retained for:
    # Either being processed for first time.
    # Being processed again, for aggregation with new data.
    # After the timeout, the data is just released for garbage collection.

    # We set this to the maximum amount of micro-batches we allow before considering data
    # old and dumping it times the time_step_interval (in which each of these micro-batches will arrive).
    ssc.remember(max_micro_batches * time_step_interval)

    # 3. We model the ssc.
    # This is the main function of the Spark application:
    # On it we specify what do we want the SparkStreaming context to do once it receives data
    # (i.e., the full set of transformations and ouptut operations we want it to perform).
    my_model(ssc, monitoring_dir, result_dir, time_step_interval, window_duration, sliding_duration)

    # 4. We return the ssc configured and modelled.
    return ssc


# ------------------------------------------
# FUNCTION get_source_dir_file_names
# ------------------------------------------
def get_source_dir_file_names(local_False_databricks_True, source_dir, valid_files, verbose):
    # 1. We create the output variable
    res = []

    # 2. We get the FileInfo representation of the files of source_dir
    fileInfo_objects = []
    if local_False_databricks_True == False:
        fileInfo_objects = os.listdir(source_dir)
    else:
        fileInfo_objects = dbutils.fs.ls(source_dir)

    # 3. We traverse the fileInfo objects, to get the name of each file
    for item in fileInfo_objects:
        # 3.1. We get a string representation of the fileInfo
        file_name = str(item)

        # 3.2. If the file is processed in DBFS
        if local_False_databricks_True == True:
            # 3.2.1. We look for the pattern name= to remove all useless info from the start
            lb_index = file_name.index("name='")
            file_name = file_name[(lb_index + 6):]

            # 3.2.2. We look for the pattern ') to remove all useless info from the end
            ub_index = file_name.index("',")
            file_name = file_name[:ub_index]

        # 3.3. If file_name is a valid_file then we append the name to the list
        if (file_name in valid_files):
            res.append(file_name)
            if verbose == True:
                print(file_name)

    # 4. We sort the list in alphabetic order
    res.sort()

    # 5. We return res
    return res


# ------------------------------------------
# FUNCTION streaming_simulation
# ------------------------------------------
def streaming_simulation(local_False_databricks_True, source_dir, monitoring_dir, time_step_interval, valid_files, verbose):
    # 1. We get the names of the files on source_dir
    files = get_source_dir_file_names(local_False_databricks_True, source_dir, valid_files, verbose)

    # 2. We get the starting time of the process
    time.sleep(time_step_interval * 0.1)

    start = time.time()

    # 2.1. If verbose mode, we inform of the starting time
    if (verbose == True):
        print("Start time = " + str(start))

    # 3. We set a counter in the amount of files being transferred
    count = 0

    # 4. We simulate the dynamic arriving of such these files from source_dir to dataset_dir
    # (i.e, the files are moved one by one for each time period, simulating their generation).
    for file in files:
        # 4.1. We copy the file from source_dir to dataset_dir#
        if local_False_databricks_True == False:
            shutil.copyfile(source_dir + file, monitoring_dir + file)
        else:
            dbutils.fs.cp(source_dir + file, monitoring_dir + file)

        # 4.2. We increase the counter, as we have transferred a new file
        count = count + 1

        # 4.3. If verbose mode, we inform from such transferrence and the current time.
        if (verbose == True):
            print("File " + str(count) + " transferred. Time since start = " + str(time.time() - start))

            # 4.4. We wait the desired transfer_interval until next time slot.
        time.sleep((start + (count * time_step_interval)) - time.time())


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc,
            local_False_databricks_True,
            source_dir,
            monitoring_dir,
            checkpoint_dir,
            result_dir,
            max_micro_batches,
            time_step_interval,
            verbose,
            window_duration,
            sliding_duration,
            race_conditions_extra_delay,
            valid_files
            ):
    # 1. We setup the Spark Streaming context
    # This sets up the computation that will be done when the system receives data.
    ssc = pyspark.streaming.StreamingContext.getActiveOrCreate(checkpoint_dir,
                                                               lambda: create_ssc(sc,
                                                                                  monitoring_dir,
                                                                                  result_dir,
                                                                                  max_micro_batches,
                                                                                  time_step_interval,
                                                                                  window_duration,
                                                                                  sliding_duration
                                                                                  )
                                                               )

    # 2. We start the Spark Streaming Context in the background to start receiving data.
    # Spark Streaming will start scheduling Spark jobs in a separate thread.

    # Very important: Please note a Streaming context can be started only once.
    # Moreover, it must be started only once we have fully specified what do we want it to do
    # when it receives data (i.e., the full set of transformations and ouptut operations we want it
    # to perform).
    ssc.start()

    # 3. As the jobs are done in a separate thread, to keep our application (this thread) from exiting,
    # we need to call awaitTermination to wait for the streaming computation to finish.
    ssc.awaitTerminationOrTimeout(time_step_interval)

    # 4. Super interesting topic: RACE CONDITIONS
    if (race_conditions_extra_delay == True):
        time.sleep((sliding_duration - 1) * time_step_interval)

    # 5. We simulate the streaming arrival of files (i.e., one by one) from source_dir to monitoring_dir.
    streaming_simulation(local_False_databricks_True, source_dir, monitoring_dir, time_step_interval, valid_files, verbose)

    # 6. Once we have transferred all files and processed them, we are done.
    # Thus, we stop the Spark Streaming Context
    ssc.stop(stopSparkContext=False)

    # 7. Extra security stop command: It acts directly over the Java Virtual Machine,
    # in case the Spark Streaming context was not fully stopped.

    # This is crucial to avoid a Spark application working on the background.
    # For example, Databricks, on its private version, charges per cluster nodes (virtual machines)
    # and hours of computation. If we, unintentionally, leave a Spark application working, we can
    # end up with an unexpected high bill.
    if (not sc._jvm.StreamingContext.getActive().isEmpty()):
        sc._jvm.StreamingContext.getActive().get().stop(False)


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Extra input arguments

    # 1.1. We select the subset of files of the dataset we want to simulate by streaming
    # In this case we are interested in the first week of May (1st-7th).
    # Unfortunately, we don't have data for May 6th, so we can exclude it.
    valid_files = ["bikeMon_20170501.csv",
                   "bikeMon_20170502.csv",
                   "bikeMon_20170503.csv",
                   "bikeMon_20170504.csv",
                   "bikeMon_20170505.csv",
                   "bikeMon_20170507.csv"
                  ]

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../"
    my_databricks_path = "/FileStore/tables/A02/"

    source_dir = "my_dataset/"
    monitoring_dir = "my_monitoring/"
    checkpoint_dir = "my_checkpoint/"
    result_dir = "my_result/"

    if local_False_databricks_True == False:
        source_dir = my_local_path + source_dir
        monitoring_dir = my_local_path + monitoring_dir
        checkpoint_dir = my_local_path + checkpoint_dir
        result_dir = my_local_path + result_dir
    else:
        source_dir = my_databricks_path + source_dir
        monitoring_dir = my_databricks_path + monitoring_dir
        checkpoint_dir = my_databricks_path + checkpoint_dir
        result_dir = my_databricks_path + result_dir

    # 4. We set the Spark Streaming parameters

    # 4.1. We specify the number of micro-batches (i.e., files) of our dataset.
    dataset_micro_batches = 6

    # 4.2. We specify the time interval each of our micro-batches (files) appear for its processing.
    time_step_interval = 10

    # 4.3. We specify the maximum amount of micro-batches that we want to allow before considering data
    # old and dumping it.
    max_micro_batches = dataset_micro_batches + 1

    # 4.4. We configure verbosity during the program run
    verbose = False

    # 4.5. window_duration, i.e., how many previous batches of data are considered on each window.
    window_duration = 2

    # 4.6. sliding duration, i.e., how frequently the new DStream computes results.
    sliding_duration = 1

    # 4.7. RACE Conditions: Discussed above. Basically, in which moment of the sliding_window do I want to start.
    # This performs an extra delay at the start of the file transferred to sync SparkContext with file transferrence.
    race_conditions_extra_delay = True

    # 5. We remove the directories
    if local_False_databricks_True == False:
        # 5.1. We remove the monitoring_dir
        if os.path.exists(monitoring_dir):
            shutil.rmtree(monitoring_dir)

        # 5.2. We remove the result_dir
        if os.path.exists(result_dir):
            shutil.rmtree(result_dir)

        # 5.3. We remove the checkpoint_dir
        if os.path.exists(checkpoint_dir):
            shutil.rmtree(checkpoint_dir)
    else:
        # 5.1. We remove the monitoring_dir
        dbutils.fs.rm(monitoring_dir, True)

        # 5.2. We remove the result_dir
        dbutils.fs.rm(result_dir, True)

        # 5.3. We remove the checkpoint_dir
        dbutils.fs.rm(checkpoint_dir, True)

    # 6. We re-create the directories again
    if local_False_databricks_True == False:
        # 6.1. We re-create the monitoring_dir
        os.mkdir(monitoring_dir)

        # 6.2. We re-create the result_dir
        os.mkdir(result_dir)

        # 6.3. We re-create the checkpoint_dir
        os.mkdir(checkpoint_dir)
    else:
        # 6.1. We re-create the monitoring_dir
        dbutils.fs.mkdirs(monitoring_dir)

        # 6.2. We re-create the result_dir
        dbutils.fs.mkdirs(result_dir)

        # 6.3. We re-create the checkpoint_dir
        dbutils.fs.mkdirs(checkpoint_dir)

    # 7. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 8. We call to our main function
    my_main(sc,
            local_False_databricks_True,
            source_dir,
            monitoring_dir,
            checkpoint_dir,
            result_dir,
            max_micro_batches,
            time_step_interval,
            verbose,
            window_duration,
            sliding_duration,
            race_conditions_extra_delay,
            valid_files
            )

