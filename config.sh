#!/bin/bash
# config.sh

python3 -m venv .
source bin/activate
pip install pip --upgrade
pip install -r requirements.txt

