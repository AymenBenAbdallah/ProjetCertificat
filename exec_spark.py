import utils

# Parameters of Spark execution
java_application = "wordcount.java"
input_file_size = "22" #size of 2^x bits

def start_and_compute(master, master_pub_ip):
    # Cleaning previous configuration
    master.run("cd ~/sujet-tp-scale ; source stop.sh", hide='both')

    # Starting cluster and executing with parameters
    master.run(f"cd ~/sujet-tp-scale ; source comp.sh {java_application}")
    master.run(f"cd ~/sujet-tp-scale ; source generate.sh filesample.txt {input_file_size}")
    master.run("cd ~/sujet-tp-scale ; source start.sh", hide='both')
    master.run("cd ~/sujet-tp-scale ; source copy.sh")
    result = master.run("cd ~/sujet-tp-scale ; source run.sh", hide='both')

    exec_time = "Undefined"
    # Parse the result and extract the execution time
    parsed_result = result.stdout.splitlines()
    for i in range(len(parsed_result)):
        if parsed_result[i].startswith("time in ms"):
            exec_time = parsed_result[i]
            
    print("-"*30)
    print(f"Execution {exec_time}")
    print("-"*30)