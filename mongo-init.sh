#!/bin/bash

# Wait for primary node
until mongosh --host mongo0 -u admin -p adminpass --eval "print(\"waited for connection\")"; do
  sleep 2
done

# Initialize replica set
mongosh --host mongo0 -u admin -p adminpass <<EOF
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "mongo0:27017", priority: 2 },
    { _id: 1, host: "mongo1:27017", priority: 1 },
    { _id: 2, host: "mongo2:27017", priority: 1, arbiterOnly: true }
  ]
})
EOF

# Wait for replica set stabilization
sleep 10

# Create application database and user
mongosh --host mongo0/rs0 -u admin -p adminpass <<EOF
use labirinto

db.createUser({
  user: 'appuser',
  pwd: 'apppass',
  roles: [
    { role: 'readWrite', db: 'labirinto' },
    { role: 'clusterMonitor', db: 'admin' }
  ]
})

db.createCollection("game_sessions", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["player_id", "start_time", "status"],
      properties: {
        player_id: { bsonType: "int" },
        start_time: { bsonType: "date" },
        status: { enum: ["active", "completed"] }
      }
    }
  }
})
EOF

echo "MongoDB initialization complete"
