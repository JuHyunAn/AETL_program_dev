import json
import sys

mode = sys.argv[1]

data = json.load(sys.stdin)

tool = data.get("tool_name")
file_path = data.get("tool_input", {}).get("file_path", "")

# CLAUDE.md 강제 읽기
if file_path.endswith("CLAUDE.md"):
    print("ALLOW")
    exit()

# evaluator logic
if file_path.endswith(".py"):

    if mode == "pre":
        print("Sub Agent Feedback: Ensure code follows CLAUDE.md guidelines before writing.")
        exit()

    if mode == "post":
        # 실제 코드 분석 logic 추가 가능
        print("ALLOW")
        exit()

print("ALLOW")