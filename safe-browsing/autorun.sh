#! /bin/bash

base_path="/home/uowadmin/Downloads/phishing-web-crawler/safe-browsing"
source $base_path/myenv/bin/activate
echo "Time: $(date)"
echo "Activated pyenv at $base_path/myenv"
echo "Start running safe-browsing script..."
python3 /$base_path/safe-browsing.py --data_dir $base_path/data --regulators=$base_path/regulatorDomains2025-10-02-manual.csv
echo "Finished running safe-browsing script..."
