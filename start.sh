#!/bin/bash

echo "Run 'Login' service, please wait..."
source venv/bin/activate
flask --app api run --host=0.0.0.0 --port=8005
