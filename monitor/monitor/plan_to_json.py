import sys
from os import path

from plan_parser import EvaluationPlan

def main():
    if len(sys.argv) < 3:
        sys.stderr.write("Usage: python plan_to_json.py [input_path] [output_path]\n")
        exit(1)

    plan_path: str = sys.argv[1]
    if not path.isfile(plan_path):
        sys.stderr.write("Plan file not found.\n")
        exit(1)

    plan = EvaluationPlan.parse(plan_path)
    output_path = sys.argv[2]
    plan.to_json(output_path)
    
    print(f"Wrote json plan to \"{output_path}\".")

if __name__ == "__main__":
    main()
