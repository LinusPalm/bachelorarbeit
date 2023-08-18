import argparse
from dataclasses import dataclass
from json import load
from os import path
import re
from typing import Any, Dict, List

class MonitorConfig(argparse.ArgumentParser):
    def __init__(self,
                 prog=None,
                 usage=None,
                 description=None,
                 epilog=None,
                 parents=[],
                 formatter_class=argparse.HelpFormatter,
                 prefix_chars='-',
                 fromfile_prefix_chars=None,
                 argument_default=None,
                 conflict_handler='error',
                 add_help=True,
                 allow_abbrev=True) -> None:
        super().__init__(prog, usage, description, epilog, parents, formatter_class, prefix_chars, fromfile_prefix_chars, argument_default, conflict_handler, add_help, allow_abbrev)
        self._required = []

    def add_argument(self, *args, **kwargs) -> argparse.Action:
        isRequired = "required" in kwargs and not kwargs.get("argOnly", False)
        if isRequired:
            kwargs["required"] = False

        if "argOnly" in kwargs:
            del kwargs["argOnly"]

        action = super().add_argument(*args, **kwargs)

        if isRequired:
            self._required.append(action.dest)

        return action
    
    def parse_args(self):
        args = super().parse_args()
        profile_parser = ProfileParser(args)
        profile = profile_parser.parse_engine_profile(args.engine)

        options = profile_parser.variables
        missing = []
        for requiredKey in self._required:
            if requiredKey not in options or options[requiredKey] == None:
                missing.append(requiredKey)
        
        if missing:
            self.error("The following options are required: " + ", ".join(missing))

        options["engine_profile"] = profile

        return options

@dataclass(frozen=True)
class EngineProfile:
    engine_program: str
    engine_args: List[str]
    engine_cwd: str
    defaults: Dict[str, str]

class ProfileParser:
    __variablePattern = re.compile(r"\$(?:{(\w+(?:\.\w+)*)}|{{(\w+(?:\.\w+)*)}})", re.IGNORECASE)

    def __init__(self, args: Any) -> None:
        self.variables: Dict[str, Any] = { **vars(args), "baseDir": path.realpath(path.dirname(__file__) + "/..") }

    def resolve_variables(self, value: str):
        containsPath = False
        def replace(match: "re.Match[str]"):
            nonlocal containsPath
            if match.group(1) != None:
                varName = match.group(1)
            else:
                varName = match.group(2)
                containsPath = True

            if varName not in self.variables:
                raise KeyError(f"Variable {varName} does not exist.")

            result = self.variables[varName]

            if match.group(0)[0] == "\\":
                result = "\\\\" + result

            return result

        resolved = re.sub(self.__variablePattern, replace, value)
        if containsPath:
            resolved = path.realpath(resolved)

        return resolved

    def parse_engine_profile(self, fileName: str):
        with open(fileName, "r") as file:
            config: dict = load(file)

        if "defaults" in config:
            defaultValues: dict = { key: self.resolve_variables(value) for key, value in config["defaults"].items() } 
            
            merged = defaultValues.copy()
            for key, value in self.variables.items():
                if key not in merged or value != None:
                    merged[key] = value

            self.variables = merged
        else:
            defaultValues = {}

        engine_program = self.resolve_variables(config["program"])
        engine_args = [ self.resolve_variables(arg) for arg in config["arguments"] ]
        engine_cwd = self.resolve_variables(config["cwd"])

        return EngineProfile(engine_program, engine_args, engine_cwd, defaultValues)
