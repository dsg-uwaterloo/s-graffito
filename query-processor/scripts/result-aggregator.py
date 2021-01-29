#!/usr/bin/python3

import csv
import sys
import os

# total time given for the execution of a single query
TIMEOUT_IN_SECONDS = 600
# period of reporting, i.e., actual execution time between each row
REPORTING_PERIOD_IN_SECONDS = 5

def get_immediate_subdirectories(a_dir):
    return [name for name in os.listdir(a_dir)
            if os.path.isdir(os.path.join(a_dir, name))]

# read the command line arguments
if len(sys.argv) != 3:
    print('Provide input folder and output files')
    sys.exit()

# second argument is the input folder file
results_folder = sys.argv[1]
aggregated_results_file = sys.argv[2]

log_folders = get_immediate_subdirectories(results_folder)

with open(aggregated_results_file, 'w') as csv_file:
    fieldnames = ['query', 'exec', 'binding', 'window-size', 'slide-size', 'slide-count',
                  'processed-size-mean', 'processed-size-p99', 'processed-mean', 'processed-p99',
                  'total-size-mean', 'total-size-p99', 'total-mean', 'total-p99',
                  'tput', 'total-time', 'last-slide-time', 'memory']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()

    for log_folder in log_folders:
        batch_size_histogram = os.path.join(results_folder, log_folder, 'batch-size.csv')
        total_size_histogram = os.path.join(results_folder, log_folder, 'total-size.csv')
        batch_latency_histogram = os.path.join(results_folder, log_folder, 'batch-latency.csv')
        total_latency_histogram = os.path.join(results_folder, log_folder, 'total-latency.csv')
        total_time_counter = os.path.join(results_folder, log_folder, 'total-time.csv')
        memory_counter = os.path.join(results_folder, log_folder, 'memory.csv')

        print('Opening {}'.format(log_folder))

        try:
            with open(batch_size_histogram, 'r') as f:
                r_list = list(csv.reader(f))
                row = next(reversed(r_list))
                slide_count = row[1]
                slide_size_mean = row[2]
                slide_size_p99 = row[7]
                # find the second of the last
                last_slide_count = 1
                for row in r_list:
                    if row[1] == slide_count:
                        # find the last slide that reported new numbers
                        break
                    else:
                        last_slide_count += 1

            with open(total_size_histogram, 'r') as f:
                row = next(reversed(list(csv.reader(f))))
                total_slide_size_mean = row[2]
                total_slide_size_p99 = row[7]

            with open(batch_latency_histogram, 'r') as f:
                row = next(reversed(list(csv.reader(f))))
                processed_mean = row[2]
                processed_p25 = row[3]
                processed_p50 = row[4]
                processed_p75 = row[5]
                processed_p90 = row[6]
                processed_p99 = row[7]
                processed_p999 = row[8]

            with open(total_latency_histogram, 'r') as f:
                row = next(reversed(list(csv.reader(f))))
                total_mean = row[2]
                total_p99 = row[7]

            with open(memory_counter, 'r') as f:
                row = next(reversed(list(csv.reader(f))))
                memory = row[0]

            # there won't be time counter for processes that are killed due to a timeout
            try:
                with open(total_time_counter, 'r') as f:
                    row = next(reversed(list(csv.reader(f))))
                    time = row[2]
            except:
                time = TIMEOUT_IN_SECONDS

            exec_name = log_folder.split('#')[0]
            query_name = log_folder.split('#')[1]
            bindings = log_folder.split('#')[2]
            window_size = log_folder.split('#')[3]
            slide_size = log_folder.split('#')[4]
            thread_count = log_folder.split('#')[5]

            writer.writerow({
                'query' : query_name,
                'exec' : exec_name,
                'binding' : bindings,
                'window-size': window_size,
                'slide-size': slide_size,
                'slide-count': slide_count,
                'processed-size-mean' : slide_size_mean,
                'processed-size-p99' : slide_size_p99,
                'processed-mean' : processed_mean,
                'processed-p99' : processed_p99,
                'total-size-mean' : total_slide_size_mean,
                'total-size-p99' : total_slide_size_p99,
                'total-mean': total_mean,
                'total-p99': total_p99,
                'tput' : str(int(slide_count) * int(slide_size_mean) / (last_slide_count * REPORTING_PERIOD_IN_SECONDS)),
                'total-time' : time,
                'last-slide-time' : str(last_slide_count * REPORTING_PERIOD_IN_SECONDS),
                'memory': memory
            })

            print('Result aggregated for {}'.format(log_folder))

        except Exception as e:
            print('Error reading values from {}: {}'.format(log_folder, e))

print('Aggregated Results written into {}'.format(aggregated_results_file))
