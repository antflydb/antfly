#!/usr/bin/env python3
"""Check MASSIVE builders against pinned pure source APIs without importing ML.

Package carriers bypass the root, inference and classification __init__ files.
Schema, constraint and compiler implementations are loaded unchanged.
Facade signatures and configuration/result dataclasses use exact source AST.
This does not execute tokenization, model scoring or numerical decoding.
"""
from __future__ import annotations

import argparse
import ast
import contextlib
import dataclasses
import importlib
from pathlib import Path
import sys
import types

import evaluate_massive11 as runner
import massive_eval_contract as execution
import oracle
import prepare_massive11 as massive


@contextlib.contextmanager
def no_numerical_imports():
    class Blocker:
        def find_spec(self, fullname, path=None, target=None):
            execution.check(fullname.split(".")[0] not in ("torch", "peft", "transformers"),
                            "numerical import forbidden in pure source audit: " + fullname)
    blocker = Blocker()
    sys.meta_path.insert(0, blocker)
    try:
        yield
    finally:
        sys.meta_path.remove(blocker)


def carrier(name, source):
    execution.check(name not in sys.modules, "source package was already imported")
    module = types.ModuleType(name)
    module.__path__ = [str(source / name.replace(".", "/"))]
    sys.modules[name] = module
    return module


def configuration_class(source):
    tree = ast.parse((source / "gliner2/classification/engine.py").read_bytes())
    classes = {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}
    methods = {node.name: node for node in classes["Classifier"].body if isinstance(node, ast.FunctionDef)}
    for name, positional in (("compile_schema", ("self", "schema")), ("score", ("self", "text", "schema")),
                             ("decode", ("self", "scores", "schema"))):
        method = methods[name]
        execution.check(tuple(arg.arg for arg in method.args.args) == positional, "Classifier API positional arguments changed")
        if name != "compile_schema":
            execution.check("config" in {arg.arg for arg in method.args.kwonlyargs}, "Classifier config argument changed")
    constants = [node for node in tree.body if isinstance(node, ast.Assign) and
                 any(isinstance(target, ast.Name) and target.id in ("_DECODERS", "_ON_INFEASIBLE") for target in node.targets)]
    for node in constants:
        ast.literal_eval(node.value)
    module = types.ModuleType("_massive_source_configuration_probe")
    module.__dict__["dataclass"] = dataclasses.dataclass
    sys.modules[module.__name__] = module
    unit = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0),
                           *constants, classes["ClassificationConfig"]], type_ignores=[])
    exec(compile(ast.fix_missing_locations(unit), str(source / "gliner2/classification/engine.py"), "exec"), module.__dict__)
    return module.ClassificationConfig


def result_classes(source, errors):
    path = source / "gliner2/classification/result.py"
    tree = ast.parse(path.read_bytes())
    classes = [node for node in tree.body if isinstance(node, ast.ClassDef) and
               node.name in ("TaskResult", "ClassificationResult")]
    execution.check(len(classes) == 2, "classification result classes changed")
    module = types.ModuleType("_massive_source_results_probe")
    module.__dict__.update(dataclass=dataclasses.dataclass, MappingProxyType=types.MappingProxyType,
                           SchemaError=errors.SchemaError)
    sys.modules[module.__name__] = module
    unit = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0),
                           *classes], type_ignores=[])
    exec(compile(ast.fix_missing_locations(unit), str(path), "exec"), module.__dict__)
    return module


def inspect(source: Path):
    execution.check(not any(name in sys.modules for name in ("gliner2", "torch", "peft")), "source audit requires a fresh process")
    source = source.resolve()
    source_identity = oracle.verify_upstream_checkout(source)
    dependencies = oracle.verify_dependencies()
    closure = execution.contract_files()
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(source))
    with runner.offline_network(), no_numerical_imports():
        gliner2 = carrier("gliner2", source)
        carrier("gliner2.inference", source)
        inference_schema = importlib.import_module("gliner2.inference.schema")
        gliner2.Schema = inference_schema.Schema
        gliner2.AttributeGroup = inference_schema.AttributeGroup
        public_tree = ast.parse((source / "gliner2/__init__.py").read_bytes())
        execution.check(any(isinstance(node, ast.ImportFrom) and node.level == 1 and node.module == "inference.schema" and
                            any(alias.name == "Schema" and alias.asname is None for alias in node.names)
                            for node in public_tree.body), "public Schema export changed")
        lazy = [node.value for node in public_tree.body if isinstance(node, ast.Assign) and
                any(isinstance(target, ast.Name) and target.id == "_LAZY" for target in node.targets)]
        execution.check(len(lazy) == 1 and ast.literal_eval(lazy[0])["AttributeGroup"] == ("gliner2.inference.schema", "AttributeGroup"),
                        "public AttributeGroup alias changed")
        classification = carrier("gliner2.classification", source)
        schema_module = importlib.import_module("gliner2.classification.schema")
        classification.ClassificationSchema = schema_module.ClassificationSchema
        compiler = importlib.import_module("gliner2.classification.compiler")
        constraints = importlib.import_module("gliner2.classification.constraints")
        results = result_classes(source, importlib.import_module("gliner2.classification.errors"))
        # Verify the exact public export our worker uses before bypassing eager
        # numerical imports. The aliased class above is this same source class.
        init_tree = ast.parse((source / "gliner2/classification/__init__.py").read_bytes())
        execution.check(any(isinstance(node, ast.ImportFrom) and node.level == 1 and node.module == "schema" and
                            any(alias.name == "ClassificationSchema" and alias.asname is None for alias in node.names)
                            for node in init_tree.body), "public ClassificationSchema export changed")
        cfg = configuration_class(source)(decoder="auto", on_infeasible="raise", batch_size=1, max_len=None,
            exact_node_budget=execution.LIMITS["exact_node_budget"], beam_size=execution.LIMITS["beam_width"],
            candidate_threshold=0.5, max_candidates_per_task=execution.LIMITS["max_candidates_per_task"], include_confidence=True)
        execution.check(cfg.on_infeasible == "raise" and cfg.max_len is None, "strict untruncated source configuration differs")
        registry, manifest = execution.load_registry(), execution.read_json(massive.MANIFEST)
        profiles = []
        for entry in registry["profiles"]:
            profile = entry["profile"]
            specification, _ = massive.schema_and_metrics(profile, manifest)
            schema = runner.python_schema(specification)
            constrained = profile["task"] == "intent_scenario"
            compiled = compiler.compile_schema(schema) if constrained else schema
            built = compiled.build()
            evidence = {"profile": profile["id"], "schema_sha256": entry["schema_sha256"],
                        "source_model_schema_sha256": execution.contract.digest(execution.contract.encoded(built))}
            if profile["task"] == "entities":
                execution.check(list(built["entities"]) == specification["entities"] and len(built["entities"]) == 55 and
                                built["classifications"] == [], "source entity ontology or ordering differs")
                evidence["entity_types"] = 55
            else:
                expected = specification["classifications"]
                execution.check(len(built["classifications"]) == len(expected), "classification task count differs")
                for declaration, emitted in zip(expected, built["classifications"], strict=True):
                    execution.check(emitted == {"task": declaration["name"], "labels": declaration["labels"],
                        "true_label": ["N/A"], "multi_label": False, "cls_threshold": 0.5, "class_act": "softmax"},
                        "source classification prompt, activation or label order differs")
                evidence["classification_labels"] = sum(len(task["labels"]) for task in expected)
                if constrained:
                    execution.check(len(compiled.constraints) == 60 and compiled.task_order == ("intent", "scenario"),
                                    "compiled constraint count/task order differs")
                    allowed = []
                    for intent in manifest["intents"]:
                        for scenario in manifest["scenarios"]:
                            selected = {"intent": {intent}, "scenario": {scenario}}
                            assignment = constraints.DictAssignment(compiled, selected, decided=compiled.task_order)
                            valid = all(constraint.evaluate(assignment) is True for constraint in compiled.constraints)
                            execution.check(valid == (manifest["intent_to_scenario"][intent] == scenario),
                                            "actual source AST differs from training-derived mapping")
                            if valid:
                                allowed.append((intent, scenario))
                    evidence.update(constraints=60, complete_assignments_checked=1080, feasible_assignments=len(allowed),
                                    source_fingerprint=compiled.fingerprint)
                    intent, scenario = allowed[0]
                    tasks = {name: results.TaskResult(task=name, labels=(label,), probabilities={value: .75 if value == label else .25
                        for value in compiled.task(name).label_names}, utilities={}, confidence=.75, exclusive=True, ordered=False, level=None)
                        for name, label in (("intent", intent), ("scenario", scenario))}
                    actual_output = results.ClassificationResult(text="schema-only probe", tasks=tasks, feasible=True,
                        violations=(), objective=0., decoder="exact", exact=True).to_dict()
                    _, canonical = execution.canonical_output({"text": "schema-only probe", "schema": specification,
                        "options": dict(manifest["request_defaults"], word_splitter=profile["word_splitter"]),
                        "offset_unit": "utf8_bytes"}, actual_output, "python", profile)
                    execution.check(canonical == {"classifications": [
                        {"name": "intent", "label": intent, "confidence": .75},
                        {"name": "scenario", "label": scenario, "confidence": .75}]}, "actual source result serialization is incompatible")
                    evidence["actual_result_dataclass_to_dict_accepted"] = True
            profiles.append(evidence)
        imported = {}
        for name, module in tuple(sys.modules.items()):
            if name == "gliner2" or name.startswith("gliner2."):
                filename = oracle.verify_import_source(module, source)
                if not filename.startswith("namespace:"):
                    path = Path(filename)
                    imported[path.relative_to(source).as_posix()] = execution.pin(path)
        for relative in ("gliner2/__init__.py", "gliner2/classification/__init__.py", "gliner2/classification/engine.py", "gliner2/classification/result.py"):
            imported[relative] = execution.pin(source / relative)
    execution.check(not any(name in sys.modules for name in ("torch", "peft")), "schema audit imported numerical runtime")
    execution.check(execution.contract_files() == closure, "execution helpers changed during source audit")
    oracle.verify_upstream_checkout(source)
    return {"scope": "gliner25_massive11_pure_source_schema_compatibility/v1", "qualification": False,
        "model_execution": False, "numerical_runtime_imported": False, "tokenization_or_scoring_executed": False,
        "source": source_identity, "source_files": imported, "dependency_versions": dependencies,
        "contract_files": closure, "generator_sha256": oracle.sha256_file(Path(__file__)), "profiles": profiles,
        "configuration": dataclasses.asdict(cfg),
        "scope_note": "Unmodified pure schema/compiler/constraint modules; root/inference/classification package carriers with AST-verified public aliases; exact AST config/result classes and facade signatures. No model, decoder search, score or token proof."}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = inspect(args.upstream)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    execution.atomic_json(args.output, result)
    print(execution.contract.encoded({"status": "verified", "scope": result["scope"], "qualification": False,
                                      "report_sha256": oracle.sha256_file(args.output)}).decode())
