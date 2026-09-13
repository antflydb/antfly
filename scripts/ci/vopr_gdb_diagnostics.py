"""GDB script: inspect a retained executable without changing replay choices.

Load with `gdb -batch -x scripts/ci/vopr_gdb_diagnostics.py --args vopr replay ...`.
This intentionally runs with GDB's embedded Python, not the system interpreter.
"""

import gdb

production_fixture = None
inspection_failed = False
tasks_inspected = False


def request_details():
    """Expose request ownership beyond std.Io's queue/select wrapper frames."""
    frame = gdb.newest_frame()
    for _ in range(48):
        if frame is None:
            break
        name = frame.name() or ""
        if name == "vopr_io_task.Entry.call":
            break  # Synthetic fiber stacks have no caller beyond their entry.
        if name.startswith("client.client.") and (
            "executeRequestCancellable" in name or "waitForRequestCancellation" in name
        ):
            gdb.write(f"  request frame: {name}\n")
            for variable in (
                "timeout_ms",
                "request_timeout_ms",
                "deadline_ms",
                "deadline_ns",
            ):
                try:
                    gdb.write(f"    {variable}={frame.read_var(variable)}\n")
                except (gdb.error, ValueError):
                    pass  # Optimized-out values are not evidence of a timeout.
            try:
                request = frame.read_var("req").dereference()
                uri = request["uri"]["raw"]
                length = min(int(uri["len"]), 512)
                raw = gdb.selected_inferior().read_memory(int(uri["ptr"]), length)
                gdb.write(f"    request={request['method']} {bytes(raw)!r}\n")
            except (gdb.error, ValueError) as error:
                gdb.write(f"    request details unavailable: {error}\n")
        frame = frame.older()


def suspended_tasks(fixture):
    """Read saved fiber stacks before teardown destroys the retained owners."""
    runtime = fixture.dereference()["sim"].dereference()
    gdb.write(f"VOPR cutoff monotonic_ns={runtime['monotonic_ns']}\n")
    tasks = runtime["tasks"]["tasks"]["items"]
    for index in range(int(tasks["len"])):
        task = tasks["ptr"][index].dereference()
        status = str(task["status"])
        if status == "finished":
            continue
        gdb.write(
            f"VOPR pending task={task['id']} scope={task['identity_parent']} "
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
            # Queue/select helpers can consume the first twelve frames. Keep
            # enough stack to identify the production caller owning the wait.
            gdb.execute("bt 48")
            request_details()
        finally:
            for name, value in saved.items():
                gdb.execute(f"set ${name} = {value}")
            gdb.invalidate_cached_frames()


def boundary():
    global production_fixture, inspection_failed, tasks_inspected
    frame = gdb.newest_frame()
    gdb.write(f"VOPR boundary: {frame.name()}\n")
    if frame.name() in (
        "vopr.full_cluster.HAScalingScenario.finalize",
        "vopr.full_cluster.Scenario.deinit",
    ):
        # Version 2 releases production owners during finalization. Inspect
        # there, before deinit can encounter the already-freed fixture. Older
        # retained executables have no finalizer and still use deinit.
        if tasks_inspected:
            return
        tasks_inspected = True
        try:
            if production_fixture is None:
                gdb.write("  no production fixture reached before teardown\n")
            else:
                suspended_tasks(production_fixture)
        except (gdb.error, ValueError) as error:
            inspection_failed = True
            gdb.write(f"  suspended task inspection unavailable: {error}\n")
        return
    try:
        pointer = frame.read_var("self")
        owner = pointer.dereference()
        fields = {field.name for field in owner.type.fields()}
        if "ha_scaling_stage" in fields:
            # Preserve the pointer value while its frame is live. Reading World
            # through optimized debug information at deinit is unreliable.
            production_fixture = gdb.Value(int(pointer)).cast(pointer.type)
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
    "full_cluster.HAScalingScenario.finalize",
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
    gdb.execute(
        f"quit {int(gdb.parse_and_eval('$_exitcode')) or int(inspection_failed)}"
    )
