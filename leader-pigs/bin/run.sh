#!/bin/bash

echo "Ensure the ./bin/pig_server.sh is running if there are problems."

mvn scala:run -Dlanucher=stats
