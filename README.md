# phishing-web-crawler
crawl phising web sites and archive them

# system deps
sudo apt-get update
sudo apt-get install -y python3 python3-venv

# repo
sudo mkdir -p /opt/fma-crawler
sudo chown -R "$USER":"$USER" /opt/fma-crawler
cd /opt/fma-crawler

python3 -m venv venv
./venv/bin/pip install -U pip
./venv/bin/pip install -r requirements.txt

# Enable and start
sudo cp systemd/fma-crawler.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now fma-crawler
journalctl -u fma-crawler -f
