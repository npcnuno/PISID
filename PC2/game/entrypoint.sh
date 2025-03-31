#!/bin/bash
set -e

export DISPLAY=:99
export WINEDEBUG=-all

echo "Starting Xvfb on display :99..."
Xvfb :99 -screen 0 1024x768x24 &
sleep 30

PLAYER=${PLAYER_ID:-33}

echo "Launching mqtt_to_mongodb for player: $PLAYER"

# Start the MQTT-to-MongoDB process in the background

sleep 4

echo "Launching game.exe for player: $PLAYER..."
# Start the Wine game in the background
wine /app/game.exe $PLAYER

# Wait for both processes to finish
