#!/bin/sh
set -e
git fetch origin
CI_DEFAULT_BRANCH_SHORT_SHA="$(git rev-parse --short=8 origin/$CI_DEFAULT_BRANCH)" # We need the short SHA char (which has 8 characters)
MAIN_FILE=../../coverage-$CI_DEFAULT_BRANCH-$CI_DEFAULT_BRANCH_SHORT_SHA.json
MR_FILE=../../coverage-$CI_COMMIT_REF_SLUG-$CI_COMMIT_SHORT_SHA.json


if test -f "$MAIN_FILE"; then
  echo "Found report on main"
else
  echo "No report found on main."
  echo "$MAIN_FILE"
  exit 0 # No need to check!
fi

if test -f "$MR_FILE"; then
  echo "Found report on this MR"
else
  echo "No report found on this branch"
  exit 1 # Should hot happen!
fi

python3 coverage/check_coverage.py $MAIN_FILE $MR_FILE
