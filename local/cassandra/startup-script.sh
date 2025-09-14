#!/bin/sh

# Start Cassandra in the background
sh /opt/cassandra/bin/cassandra -f &

while ! grep -q "Startup complete" /opt/cassandra/logs/system.log
do
  echo "Waiting for Cassandra to start..."
  sleep 1
done;

# Create sample table
# cqlsh -f $DEBEZIUM_HOME/inventory.cql

# Run Cassandra Connector
java -Dlog4j.debug \
     -Dlog4j.configuration=file:$DEBEZIUM_HOME/log4j.properties \
     --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
     --add-exports java.base/jdk.internal.ref=ALL-UNNAMED \
     --add-exports java.base/sun.nio.ch=ALL-UNNAMED \
     --add-exports java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED \
     --add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED \
     --add-exports java.rmi/sun.rmi.server=ALL-UNNAMED \
     --add-exports java.sql/java.sql=ALL-UNNAMED \
     --add-opens java.base/java.lang.module=ALL-UNNAMED \
     --add-opens java.base/jdk.internal.loader=ALL-UNNAMED \
     --add-opens java.base/jdk.internal.ref=ALL-UNNAMED \
     --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED \
     --add-opens java.base/jdk.internal.math=ALL-UNNAMED \
     --add-opens java.base/jdk.internal.module=ALL-UNNAMED \
     --add-opens java.base/jdk.internal.util.jar=ALL-UNNAMED \
     --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED \
     --add-opens=java.base/java.io=ALL-UNNAMED \
     -jar $DEBEZIUM_HOME/debezium-connector-cassandra.jar $DEBEZIUM_HOME/config.properties