#!/bin/bash
# download
wget https://bin.equinox.io/c/4VmDzA7iaHb/ngrok-stable-linux-amd64.zip

# unzip
unzip ngrok-stable-linux-amd64.zip 

# move to /usr/local/bin
sudo mv ngrok /usr/local/bin

# create directory
sudo mkdir -p /opt/ngrok/systemd/system/
setup="$(pwd)/installer/ngrok/config/pc4/ngrok.yml"
echo $setup
# create systemd script
echo "[Unit]
Description=ngrok script
[Service]
ExecStart=/usr/local/bin/ngrok start --all --region=ap --config=$setup
Restart=always
[Install]
WantedBy=multi-user.target" | sudo tee /opt/ngrok/systemd/system/ngrok.service

# copy systemd script 
sudo cp /opt/ngrok/systemd/system/ngrok.service /etc/systemd/system/ngrok.service

# reload daemon
sudo systemctl daemon-reload

# enable service
sudo systemctl enable ngrok.service

# start service
sudo systemctl start ngrok.service