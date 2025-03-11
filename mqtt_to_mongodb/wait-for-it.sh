#!/bin/sh

set -e

hostport="$1"
shift

while ! mongosh --host ${hostport%:*} -u admin -p adminpass --eval "db.runCommand('ping').ok" >/dev/null 2>&1; do
  echo "Waiting for MongoDB at $hostport..."
  sleep 2
done

echo "MongoDB is ready - executing command: $@"
exec "$@"
