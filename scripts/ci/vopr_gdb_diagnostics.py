"""GDB script: inspect a retained executable without changing replay choices.

Load with `gdb -batch -x scripts/ci/vopr_gdb_diagnostics.py --args vopr replay ...`.
This intentionally runs with GDB's embedded Python, not the system interpreter.
"""

import os

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


def repair_attempt():
    """Retain repair state behind a coarse materialization timeout."""
    frame = gdb.newest_frame()
    gdb.write(f"VOPR repair boundary: {frame.name()}\n")
    for name in ("repair_id", "terminal", "err_name"):
        try:
            value = frame.read_var(name)
            if name == "err_name":
                length = min(int(value["len"]), 512)
                value = bytes(
                    gdb.selected_inferior().read_memory(int(value["ptr"]), length)
                )
            gdb.write(f"  {name}={value}\n")
        except (gdb.error, ValueError):
            pass
    try:
        intent = frame.read_var("entry").dereference()["intent"]
        for name in (
            "phase",
            "attempt_count",
            "failure_streak",
            "next_retry_at_ms",
            "last_error",
        ):
            gdb.write(f"  {name}={intent[name]}\n")
    except (gdb.error, ValueError):
        pass


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


def application_error():
    """Retain private error identity behind a generic public HTTP 500.

    Test-linked VOPR executables suppress HTTP stderr to preserve Zig's test
    protocol. Inspect the error at ingress, before its task unwinds or teardown
    releases the request, without exposing internal details in the API body.
    """
    frame = gdb.newest_frame()
    gdb.write(f"VOPR HTTP error boundary: {frame.name()}\n")
    try:
        value = frame.read_var("err")
        gdb.write(f"  error={value} numeric={int(value)} type={value.type}\n")
        gdb.execute("ptype err")
    except (gdb.error, ValueError) as error:
        gdb.write(f"  error identity unavailable: {error}\n")
    # Keep the calling convention evidence as well as optimized DWARF values.
    # Zig error unions can otherwise render only their `err` discriminant.
    gdb.execute("info registers rax rbx rcx rdx rsi rdi r8 r9")
    gdb.execute("x/16i $pc")
    try:
        owner = frame
        while owner is not None:
            try:
                context = owner.read_var("ctx")
                break
            except (gdb.error, ValueError):
                owner = owner.older()
        else:
            raise ValueError("request context unavailable on transport stack")
        if context.type.code == gdb.TYPE_CODE_PTR:
            context = context.dereference()
        request = context["request"]
        if request.type.code == gdb.TYPE_CODE_PTR:
            request = request.dereference()
        uri = request["uri"]["raw"]
        raw = gdb.selected_inferior().read_memory(
            int(uri["ptr"]), min(int(uri["len"]), 512)
        )
        gdb.write(f"  request={request['method']} {bytes(raw)!r}\n")
    except (gdb.error, ValueError) as error:
        gdb.write(f"  request unavailable: {error}\n")
    gdb.execute("bt 12")


def boundary():
    global production_fixture, inspection_failed, tasks_inspected
    frame = gdb.newest_frame()
    gdb.write(f"VOPR boundary: {frame.name()}\n")
    frame_name = frame.name() or ""
    if (
        frame_name.endswith("ScalingScenario.finalize")
        or frame_name == "vopr.full_cluster.Scenario.deinit"
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
        # Retained executables may predate the standby naming. Discover the
        # field from their debug information without changing replay identities.
        stage_field = next(
            (name for name in fields if name and name.endswith("_scaling_stage")), None
        )
        if stage_field is not None:
            # Preserve the pointer value while its frame is live. Reading World
            # through optimized debug information at deinit is unreliable.
            production_fixture = gdb.Value(int(pointer)).cast(pointer.type)
        for name in (
            stage_field,
            "phase",
            "driver_rounds",
            "control_round_active",
            "raft_driver_active",
            "data_server_paused",
            "data_server_live",
        ):
            if name is not None and name in fields:
                gdb.write(f"  {name}={owner[name]}\n")
    except gdb.error as error:
        gdb.write(f"  owner unavailable: {error}\n")
    gdb.execute("bt 3")


gdb.execute("set pagination off")
gdb.execute("set confirm off")
gdb.execute("set print elements 12")
gdb.execute("set breakpoint pending on")
for expression in (
    "DB.recordIndexRepairAttemptFailure",
    "DB.advanceIndexRepairIntentOwned",
):
    for breakpoint in gdb.rbreak(expression):
        breakpoint.silent = True
        breakpoint.commands = "silent\npython repair_attempt()\ncontinue\n"
# These are infrequent ownership/control boundaries, not scheduler steps.
for expression in (
    "production_cluster.*[A-Za-z]+Reconcile",
    "production_cluster.*[A-Za-z]+SetReplicaCount",
    "stopDataServerForRestart",
    "restartDataServer",
    "production_cluster.*run[A-Za-z]+Scaling",
    "full_cluster.*ScalingScenario.finalize",
    "full_cluster.Scenario.deinit",
    "production_cluster.*beginTeardown",
    "production_.*Owners.*(startPrimary|startStandby|catchUp|write|verify|promote)",
):
    for breakpoint in gdb.rbreak(expression):
        breakpoint.silent = True
        breakpoint.commands = "silent\npython boundary()\ncontinue\n"

# The transport boundary survives ReleaseSafe inlining of ingress handlers.
# Resolve symbols from the retained executable, never source line numbers from
# the diagnostic checkout (which can belong to a different revision).
http_boundaries = gdb.rbreak("routeErrorResponseStatus")
if not http_boundaries:
    raise gdb.GdbError("No HTTP transport error boundary found in retained executable")
http_boundaries += gdb.rbreak("AntflyApiHandler.mapIngressError")
for breakpoint in http_boundaries:
    breakpoint.silent = True
    breakpoint.commands = "silent\npython application_error()\ncontinue\n"


replays = int(os.environ.get("VOPR_DIAGNOSTIC_REPLAYS", "1"))
if not 1 <= replays <= 200:
    raise gdb.GdbError("VOPR_DIAGNOSTIC_REPLAYS must be between 1 and 200")
for iteration in range(1, replays + 1):
    production_fixture = None
    inspection_failed = False
    tasks_inspected = False
    gdb.write(f"VOPR diagnostic replay {iteration}/{replays}\n")
    gdb.execute("run")
    # Each replay starts a new process. Keep the loaded symbols/breakpoints,
    # never process state, and stop on the first failing execution.
    if gdb.selected_inferior().pid:
        gdb.execute("thread apply all bt 12")
        gdb.execute("quit 1")
    status = int(gdb.parse_and_eval("$_exitcode")) or int(inspection_failed)
    if status:
        gdb.execute(f"quit {status}")
gdb.execute("quit 0")
