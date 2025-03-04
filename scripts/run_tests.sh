#!/bin/bash
#
# Run project tests
#
# NOTE: This script expects to be run from the project root with
# ./scripts/run_tests.sh

set -o pipefail

function display_result {
  RESULT=$1
  EXIT_STATUS=$2
  TEST=$3

  if [ $RESULT -ne 0 ]; then
    echo -e "\033[31m$TEST failed\033[0m"
    exit $EXIT_STATUS
  else
    echo -e "\033[32m$TEST passed\033[0m"
  fi
}

if [[ -z "$VIRTUAL_ENV" ]] && [[ -d venv ]]; then
  source ./venv/bin/activate
fi

make test-requirements
display_result $? 1 "Requirements check"

flake8 .
display_result $? 1 "Code style check"

isort --check-only ./app ./tests
display_result $? 2 "Import order check"

# run with four concurrent threads
py.test -n4 -v --maxfail=10
display_result $? 2 "Unit tests"
