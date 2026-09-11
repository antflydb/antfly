"""GDB script: inspect a retained executable without changing replay choices.

Load with `gdb -batch -x scripts/ci/vopr_gdb_diagnostics.py --args vopr replay ...`.
This intentionally runs with GDB's embedded Python, not the system interpreter.
"""

import gdb


def suspended_tasks(world):
    """Read saved fiber stacks before teardown destroys the retained owners."""
    state = world.dereference()["state"].dereference()
    tasks = state["sim"]["tasks"]["tasks"]["items"]
    for index in range(int(tasks["len"])):
        task = tasks["ptr"][index].dereference()
        status = str(task["status"])
        if status == "finished":
            continue
        gdb.write(
            f"VOPR pending task={task['id']} parent={task['identity_parent']} "
            f"owner={task['resource_owner_id']} status={status} "
            f"start={task['start']} sleep={task['sleep']} "
            f"awaited={task['waiting_on_future']} external={task['external_id']}\n"
        )
        if not bool(task["started"]) or status == "running":
            continue
        # std.Io.fiber.Context saves rsp/rbp/rip on x86_64. Only change the
        # debugger's stopped register view, unwind, then restore it before any
        # instruction executes. No function calls or schedule changes occur.
        saved = {
            name: int(gdb.parse_and_eval(f"${name}")) for name in ("rsp", "rbp", "rip")
        }
        try:
            for name in saved:
                gdb.execute(f"set ${name} = {int(task['context'][name])}")
            gdb.invalidate_cached_frames()
            gdb.execute("bt 12")
        finally:
            for name, value in saved.items():
                gdb.execute(f"set ${name} = {value}")
            gdb.invalidate_cached_frames()


def boundary():
    frame = gdb.newest_frame()
    gdb.write(f"VOPR boundary: {frame.name()}\n")
    if frame.name() == "vopr.full_cluster.Scenario.deinit":
        try:
            suspended_tasks(frame.read_var("world"))
        except (gdb.error, ValueError) as error:
            gdb.write(f"  suspended task inspection unavailable: {error}\n")
        return
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
    "full_cluster.Scenario.deinit",
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
else:
    gdb.execute(f"quit {int(gdb.parse_and_eval('$_exitcode'))}")
