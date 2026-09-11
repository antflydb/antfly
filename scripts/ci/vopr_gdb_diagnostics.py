"""GDB script: inspect a retained executable without changing replay choices.

Load with `gdb -batch -x scripts/ci/vopr_gdb_diagnostics.py --args vopr replay ...`.
This intentionally runs with GDB's embedded Python, not the system interpreter.
"""

import gdb


def boundary():
    frame = gdb.newest_frame()
    gdb.write(f"VOPR boundary: {frame.name()}\n")
    try:
        owner = frame.read_var("self").dereference()
        fields = {field.name for field in owner.type.fields()}
        for name in (
            "ha_scaling_stage",
            "phase",
            "driver_rounds",
            "control_round_active",
            "raft_driver_active",
            "data_server_paused",
            "data_server_live",
        ):
            if name in fields:
                gdb.write(f"  {name}={owner[name]}\n")
    except gdb.error as error:
        gdb.write(f"  owner unavailable: {error}\n")
    gdb.execute("bt 3")


gdb.execute("set pagination off")
gdb.execute("set confirm off")
gdb.execute("set print elements 12")
gdb.execute("set breakpoint pending on")
# These are infrequent ownership/control boundaries, not scheduler steps.
for expression in (
    "haReconcile",
    "haSetReplicaCount",
    "stopDataServerForRestart",
    "restartDataServer",
    "runHAScaling",
    "production_cluster.*beginTeardown",
    "production_ha.*(startPrimary|startStandby|catchUp|write|verify|promote)",
):
    for breakpoint in gdb.rbreak(expression):
        breakpoint.silent = True
        breakpoint.commands = "silent\npython boundary()\ncontinue\n"

gdb.execute("run")
# GDB itself can exit successfully after an inferior crash; retain its final
# stack and propagate that distinction to the workflow.
if gdb.selected_inferior().pid:
    gdb.execute("thread apply all bt 12")
    gdb.execute("quit 1")
