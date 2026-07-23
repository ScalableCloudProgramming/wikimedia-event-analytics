#!/bin/bash
# EC2 bootstrap — all pipeline roles run on AWS (no local services)
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
export AWS_DEFAULT_REGION="__AWS_REGION__"
export HOME=/home/ubuntu

apt-get update -y
apt-get install -y python3-pip python3-venv unzip curl awscli

mkdir -p /opt/wikimedia
cd /opt/wikimedia

# Pull packaged code from S3
aws s3 cp "s3://__S3_BATCH__/deploy/pipeline-code.zip" /tmp/pipeline-code.zip
unzip -o /tmp/pipeline-code.zip -d /opt/wikimedia

# Write runtime env (instance role provides credentials)
cat > /opt/wikimedia/.env <<'ENVEOF'
__ENV_FILE__
ENVEOF

python3 -m venv /opt/wikimedia/.venv
source /opt/wikimedia/.venv/bin/activate
pip install --upgrade pip
pip install -r /opt/wikimedia/requirements.txt

ROLE="__INSTANCE_ROLE__"

# systemd unit for this instance role
if [ "$ROLE" = "producer" ]; then
cat > /etc/systemd/system/wiki-producer.service <<'EOF'
[Unit]
Description=Wikimedia high-scale producer
After=network.target
[Service]
Type=simple
WorkingDirectory=/opt/wikimedia
EnvironmentFile=/opt/wikimedia/.env
Environment=PYTHONUNBUFFERED=1
ExecStart=/opt/wikimedia/.venv/bin/python ingestion/high_scale.py
Restart=always
RestartSec=5
User=ubuntu
[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now wiki-producer.service
fi

if [ "$ROLE" = "speed" ]; then
cat > /etc/systemd/system/wiki-speed.service <<'EOF'
[Unit]
Description=Wikimedia speed layer consumer
After=network.target
[Service]
Type=simple
WorkingDirectory=/opt/wikimedia
EnvironmentFile=/opt/wikimedia/.env
Environment=PYTHONUNBUFFERED=1
ExecStart=/opt/wikimedia/.venv/bin/python speed/consumer.py
Restart=always
RestartSec=5
User=ubuntu
[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now wiki-speed.service
fi

if [ "$ROLE" = "dashboard" ]; then
cat > /etc/systemd/system/wiki-dashboard.service <<'EOF'
[Unit]
Description=Wikimedia Streamlit dashboard
After=network.target
[Service]
Type=simple
WorkingDirectory=/opt/wikimedia
EnvironmentFile=/opt/wikimedia/.env
Environment=PYTHONUNBUFFERED=1
ExecStart=/opt/wikimedia/.venv/bin/streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0 --browser.gatherUsageStats false
Restart=always
RestartSec=5
User=ubuntu
[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now wiki-dashboard.service
fi

# Mark ready
echo "ready role=$ROLE $(date -Is)" > /opt/wikimedia/READY
