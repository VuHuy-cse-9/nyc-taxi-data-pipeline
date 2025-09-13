#!/bin/bash
cmd=$1

usage() {
    echo "run.sh <command> <arguments>"
    echo "Available commands:"
    echo " register_connector          register a debezium connector"
    echo " remove_connector          remove a debezium connector"
    echo " update_connector          remove a debezium connector"
    echo "Available arguments:"
    echo " [connector config path]     path to connector config, for command register_connector only"
}

if [[ -z "$cmd" ]]; then
    echo "Missing command"
    usage
    exit 1
fi

case $cmd in
    register_connector)
        if [[ -z "$2" ]]; then
            echo "Missing connector config path"
            usage
            exit 1
        else
            echo "Registering a new connector from $2"
            # Assign a connector config path such as: kafka_connect_jdbc/configs/connect-timescaledb-sink.json
            curl -i -X POST -H "Accept:application/json" -H 'Content-Type: application/json' http://localhost:8083/connectors -d @$2
        fi
        ;;
    remove_connector)
        if [[ -z "$2" ]]; then
            echo "Missing connector name"
            usage
            exit 1
        else
            echo "Removing connector $2"
            # Assign a connector name such as: timescaledb-sink
            curl -i -X DELETE -H "Accept:application/json" http://localhost:8083/connectors/$2
        fi
        ;;
    *)
        echo -n "Unknown command: $cmd"
        usage
        exit 1
        ;;
esac