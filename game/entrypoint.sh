#!/bin/bash
set -e

# Set DISPLAY and reduce Wine debug output
export DISPLAY=:99
export WINEDEBUG=-all

echo "Starting Xvfb on display :99..."
Xvfb :99 -screen 0 1024x768x24 &
sleep 10

PLAYER=${PLAYER_ID:-33}
echo "Launching game.exe for player: $PLAYER..."
wine /app/game.exe $PLAYER
