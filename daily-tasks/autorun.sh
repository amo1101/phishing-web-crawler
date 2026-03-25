#! /bin/bash
if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <CMD> <OUTPUT_BASE>" >&2
  exit 2
fi

run_dir="/home/uowadmin/Downloads/phishing-web-crawler/daily-tasks"
cmd="$1"
output_base="$2"

setup_environment() {
    source ${run_dir}/../myenv/bin/activate
    echo "Time: $(date)"
    echo "Activated pyenv at ${run_dir}/../myenv"
}

fetch_iosco_csv() {
    echo "Start fetching iosco csv..."
    python3 $run_dir/fetch_iosco_csv.py "$output_base"
    echo "Finished fetching iosco csv..."
}

iosco_url_liveness_check() {
    echo "Start checking iosco url liveness..."
    python3 $run_dir/liveness_check.py "$output_base"
    echo "Finished checking iosco url liveness..."
}

safe_browsing_report() {
    echo "Start running safe-browsing script..."
    python3 $run_dir/safe-browsing.py --data_dir $output_base --regulators=$run_dir/regulatorDomains2025-10-02-manual.csv
    echo "Finished running safe-browsing script..."
}

setup_environment
if [[ $cmd -eq 0 ]]; then
    fetch_iosco_csv
elif [[ $cmd -eq 1 ]]; then
    iosco_url_liveness_check
elif [[ $cmd -eq 2 ]]; then
    safe_browsing_report
fi
