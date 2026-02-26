#!/bin/bash

input=$(cat)

feedback=$(echo "$input" | python .claude/agents/evaluator_agent.py post)

if [[ "$feedback" != "ALLOW" ]]; then
  jq -n --arg msg "$feedback" '{
    action: "block",
    message: $msg
  }'
else
  jq -n '{action: "allow"}'
fi