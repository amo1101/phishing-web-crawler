# phishing-web-crawler
A tool for archiving phishing websites reported by [IOSCO](https://www.iosco.org/).

📍 Self-hosting [Browsertrix](https://docs.browsertrix.com/) for live websites crawling.

📍 [wayback machine downloader](https://github.com/StrawberryMaster/wayback-machine-downloader.git) for "dead" websites downloading.


![Alt Text](https://github.com/amo1101/phishing-web-crawler/blob/main/demo.gif)

## What Phishing-web-crawler does:
1. Download the list of suspicious financial scam URLs reported by https://www.iosco.org/i-scan/, first time from a specified base date, and then incrementally on daily basis.
2. Automatically cleanse the URLs from multiple rows of exported csv to get valid URLs.
3. Check the liveness of URLs.
4. Schedule Browsertrix jobs to crawl live URLs.
    * prefix scope for domain URL
    * page scope for social media pages, or URL with parameters specified.
5. Schedule wayback machine download jobs to download the whole domain pages of the dead URLs.

## Installation

- OS: Ubuntu 22 or above

- Browsertrix

    - Method 1: Deploy browsertrix on local machine, refer to: https://docs.browsertrix.com/deploy/local. Howerver, I cannot figure out how to enable superuser account with helm3 repo, so I chose method 2.

    - Method 2: After microk8s is installed, install Browsertrix from its git repo:
        ```
        # download browsertrix git repo:
        git clone https://github.com/webrecorder/browsertrix.git

        # under the root directory:
        microk8s helm3 upgrade --install -f ./chart/values.yaml -f ./chart/local.yaml btrix ./chart

        # You may need to adjust crawler resource limit setting in chart/values.yaml, otherwise the crawler job may stuck at: "Waiting (At Capacity)" (refer to the values.yaml.patch under the phishing-web-crawler repo for configuration on my machine which is Intel i5-8600K cpu + 32G memory). After adjusting the configuration, run the above command again to enable the change.
        ```
    - Now login to the Browsertrix home page: http://localhost:30870, and login with superuser account (see "superuser" in chart/local.yaml).
    - You could use the default organization, and optionally you could create a collection.

- wayback machine downloader

    - Install wayback machine downloader on local machine (NOT docker version), refer to: https://github.com/StrawberryMaster/wayback-machine-downloader. 
    - Make sure you can run it from the command line, e.g.:
        ```
        wayback_machine_downloader --version
        ```

- Phishing web crawler

    - Download this repo and install the required packages, using python3 venv is recommended.
        
        ```
        # under the root directory of the repo
        python3 -m venv myenv
        source ./myenv/bin/activate
        pip install -r requirements.txt
        ```

    - Configure the crawler by editing config.yaml, key configrations:
        ```
        # database file path
        state_db: "/home/uowadmin/iosco/data/state.db"

        # Scheduling
        schedule:
        base_date: "2025-09-01"     # base date
        daily_run_time: "13:00"     # local time for the daily ingestion

        # Browsertrix integration
        browsertrix:
        ...
        crawler_setting:
            max_time: 86400  # max crawl time in seconds
            max_size: 10737418240 # max crawl size in bytes
            # crawler job frequency
            # crontab format: min, hour, dom, mon, dow; e.g, "0 0 * * 1" run every Monday 00:00;
            # run once if not specified
            frequency: ""
    
        # Dead-site acquisition
        wb_downloader:
        output_dir: "/scratch/iosco/wayback"  # wayback downloader storage
        concurrency: 3
        ...
        ```
    - Start the crawler
        ```
        python3 -m crawler.main --config config.yaml
        ```
    - Start the web page to check the crawler job status:
        ```
        python3 -m crawler.webapp --config config.yaml
        ```

- Safe browser API
    - A script under the safe-browsing folder, it exports all URLs from https://www.iosco.org/i-scan/, cleanses URLs, and get Google safe browsing report for each URL.

## TODO
- The crawling setting could be further optimised, e.g., 
    * URLs require login could not be crawled,potentially could be fixed by applying browser profiles.
    * Some web site could be very huge containing lots of articles, could use more fine-grained rules to execlude thoes pages.
- WACZ file processing:
    * [waczerciser](https://github.com/harvard-lil/waczerciser): extracting WACZ files
    * [warc-gpt](https://github.com/harvard-lil/warc-gpt): RAG pipeline for web achive collections
