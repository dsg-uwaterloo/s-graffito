#!/usr/bin/python3

import json
import os
import sys
import subprocess
import time
import psutil
import csv

class TestRun:
    def __init__(self, exec_name, query_name, input, input_type, report, window_size, slide_size, thread_count, labels):
        self.query_name = query_name
        self.exec_name = exec_name
        self.input = input
        self.input_type = input_type
        self.report_file = report
        self.window_size = window_size
        self.slide_size = slide_size
        self.predicates = labels
        self.thread_count = thread_count


    def produceCommandString(self):
        command = '{} {} {} {} {} {} {} {} {}'.format(
            self.exec_name,
            str(self.window_size),
            str(self.slide_size),
            self.input_type,
            self.input,
            self.report_file,
            self.query_name,
            len(self.predicates),
            " ".join(self.predicates)
        )

        return command

# read the command line arguments
if len(sys.argv) != 2:
    print('Provide configuration file as an argument')
    sys.exit()

# second argument is the parameters file
parameters = sys.argv[1]

# list to hold all the objects for this set of experiments
run_list = []
timeout = 600
project_base = ''


# add RUST logger to environment variables
env_variables = os.environ.copy()
env_variables['RUST_LOG'] = 'info' 

# parse json files and populate Run objects
with open(parameters, 'r') as parameters_handle:
    parameters_json = json.load(parameters_handle)
    run_configs = parameters_json['runs']

    # global parameters
    dataset_location = parameters_json['dataset']
    report_folder = parameters_json['report-folder']
    timeout = parameters_json['timeout']
    project_base = parameters_json['project-base']
    input_type = parameters_json['input-type']

    # iterate overs run specific parameters
    for run_config in run_configs:
        query_name = run_config['query-name']
        index = run_config['index']
        exec_name = run_config['exec-name']
        predicates = run_config['predicates']
        window_size = run_config['window-size']
        slide_size = run_config['slide-size']
        # set thread-count to 1 by default
        thread_count = run_config.get('thread-count', 1)

        # reporting folder
        report_csv_path = os.path.join(report_folder, exec_name + '#' + query_name + '#' + str(index) + '#' + str(window_size) + '#' + str(slide_size) + '#' + str(thread_count))
        # create the run object
        run_list.append(TestRun(exec_name, query_name, dataset_location, input_type, report_csv_path, window_size, slide_size, thread_count, predicates))


# iterate over runs and run the experiments
for run in run_list:
    argument_string = run.produceCommandString()

    run_command = 'cargo run --release --example {}'.format(argument_string)

    print('Executing command {} '.format(run_command))
    sys.stdout.flush()

    elapsedTime = 0
    interval = 5

    proc = subprocess.Popen(run_command.split(), env=env_variables)

    ## array to store memory consumption readings
    memory_rss_readings = []

    # wait until program completion or termination
    while True:
        time.sleep(interval)
        elapsedTime += interval

        # obtain memory usage of the process
        try:
            process = psutil.Process(proc.pid)
            memory_rss_readings.append(process.memory_info().rss)
        except:
            print('Process pid {} is already terminated'.format(str(proc.pid)))

        # kill after timeout if process is still alive
        if elapsedTime > timeout and proc.poll() is None:

            print('Killing pid {} after timeout {}'.format(str(proc.pid), str(timeout)))
            sys.stdout.flush()
            proc.kill()
            # sleep before starting new job for java to release the memory
            time.sleep(interval)
            break

        if proc.poll() is not None:
            break
    # finally report max memory reading in the reporting directory
    if not os.path.exists(run.report_file):
        os.makedirs(run.report_file)

    print('Max memory consumption {}'.format(str(max(memory_rss_readings))))
    with open(os.path.join(run.report_file, 'memory.csv'), mode='w') as memory_csv:
        writer = csv.DictWriter(memory_csv, fieldnames=['max'])
        writer.writeheader()
        # write max memory reading
        writer.writerow({'max' : max(memory_rss_readings) })
    # close the csv file
    memory_csv.close()

print('All runs are completed')
