#!/bin/bash

mode=$1

feedback=$(python .claude/agent/evaluator_agent.py "$mode")

if [[ "$feedback" != "ALLOW" ]]; then
  jq -n --arg msg "$feedback" '{
    action: "block",
    message: $msg
  }'
else
  jq -n '{action: "allow"}'
fi
