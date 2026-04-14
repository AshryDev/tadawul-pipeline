#!/bin/bash

echo "=============================================================="
echo "          Running dbt deps to install dependencies"
dbt deps

echo "          Running dbt docs to generate documentation"
dbt docs generate

echo "=============================================================="
echo "              Finished setting up DBT project"

echo "=============================================================="
echo "          Waiting for execution outside the container"
tail -f /dev/null
