#!/bin/bash

send_discord_message() {
    if [[ -z ${EXPERIMENT_SILENT+x} ]]; then
        source_name=$(cat /etc/hostname)
        json_msg='{"content": "'$source_name': '$1'"}'
        #curl -X POST -H "Content-Type: application/json" -d "$json_msg" [INSERT_WEBHOOK_URL_HERE]
    fi
}

if [[ $# < 2 ]]; then
    echo "Usage: run_experiment experiment.csv output_dir [skip] [first_run]"
    exit 1
fi

if [[ -z ${CONDA_DEFAULT_ENV+x} ]]; then
    echo "Conda environment not activated."
    exit 1
fi

skip=3
if [[ $# > 2 ]]; then
    skip=$((3+$3))
    echo "Skipping first $3 experiments."
fi

first_run=0
if [[ $# > 3 ]]; then
    first_run=$4
    echo "Starting at run $4 for each experiment"
fi


IFS=","
read -r global_engine global_plan global_network global_pattern global_run_count global_args < <(sed -n '2p' < $1)

# Split global args into multiple args, otherwise they will be interpreted as one argument
IFS=' ' read -ra global_args <<< "$global_args"

echo "Global Settings:"
echo "Engine: $global_engine, Plan: $global_plan, Network: $global_network, Input Pattern: $global_pattern, Run Count: $global_run_count, Args: ${global_args[@]}"
echo

BASEDIR=~/ba
MONITOR=$BASEDIR/monitor/run_node.py
LOGDIR=$BASEDIR/logs

send_discord_message "Starting experiments $1. Storing results to $2. Global args: ${global_args[@]}"

while IFS="," read -r experiment_id engine plan_file network input_pattern run_count engine_args
do
    if [[ -z "$engine" ]]; then
        engine=$global_engine
    fi

    engine=$(realpath $engine)

    if [[ -z "$plan_file" ]]; then
        plan_file=$global_plan
    fi

    if [[ ! -f $plan_file ]]; then
        echo "Plan file $plan_file not found."
        exit 1
    fi

    plan_file=$(realpath $plan_file)

    if [[ -z "$network" ]]; then
        network=$global_network
    fi

    if [[ ! -f $network ]]; then
        echo "Network file $network not found."
        exit 1
    fi

    network=$(realpath $network)

    if [[ -z "$input_pattern" ]]; then
        input_pattern=$global_pattern
    fi

    if [[ -z "$run_count" ]]; then
        run_count=$global_run_count
    fi

    if [[ -z "$engine_args" ]]; then
        engine_args=("${global_args[@]}")
    else
        IFS=' ' read -ra engine_args <<< "$engine_args"
    fi

    # Count occurences of "forward_rules" (one per node) to get node count
    nodeCount=$(grep -o "forward_rules" $plan_file | wc -l)

    planDir=$(dirname $(realpath $plan_file))

    echo "$engine, $plan_file, $network, $input_pattern, $run_count, ${engine_args[@]}"

    for ((run=$first_run; run < $((run_count+first_run)); run++));
    do
        now=$(date +"%d-%m-%Y_%H%M")
        echo "Starting run $run at $now"

        run_dir="$2/$experiment_id/run_$run"
        mkdir -p $run_dir

        echo "Saving results of run $run to $run_dir"

        for ((i=0; i < $((nodeCount-1)); i++));
        do
            python3 -u $MONITOR --plan $plan_file --engine $engine --node $i --logdir $run_dir --inputFile $planDir/$(printf $input_pattern $i) --config $network --runid $now ${engine_args[@]} > $run_dir/out_${now}_$i.txt 2> $run_dir/error_${now}_$i.txt &
            pid=$!
            sleep 1
            kill -0 $pid
            result=$?

            if [[ $result != 0 ]]; then
                echo "Could not start node $i. Exit code: $result"
                send_discord_message "Could not start node $i in run $run of experiment $experiment_id."
		        cat $run_dir/error_${now}_$i.txt
                exit 1
            fi

            echo "Started node $i"
        done

        i=$((nodeCount-1))
        python3 -u $MONITOR --plan $plan_file --engine $engine --node $i --logdir $run_dir --inputFile $planDir/$(printf $input_pattern $i) --config $network --runid $now ${engine_args[@]} > $run_dir/out_${now}_$i.txt 2> $run_dir/error_${now}_$i.txt &

        pid=$!
        : > /dev/null &
        wait $pid
        wait

        report_msg="Run $run of experiment $experiment_id has finished."
        echo "$report_msg"
        send_discord_message $report_msg
    done

    echo
done < <(tail -n +$skip $1)

send_discord_message "All experiments completed."
