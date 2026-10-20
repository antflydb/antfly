"""Per-request log attribution; wall intervals are not GPU-event measurements."""
import math

GENERATION_PHASES = ('prompt_format', 'tokenize', 'multimodal_prepare', 'runtime_prepare',
                     'prefill', 'decode', 'text_decode')
PREPARE_PHASES = ('query_ns', 'snapshot_ns', 'fit_ns', 'export_write_ns')


def one_record(text, marker, fields):
    matches = [line.split(marker, 1)[1].strip() for line in text.splitlines() if marker in line]
    if len(matches) != 1: raise ValueError(f'expected one {marker} record, found {len(matches)}')
    result = {}
    for field in matches[0].split():
        key, separator, value = field.partition('=')
        if separator != '=' or key in result or not value: raise ValueError('malformed/duplicate timing field')
        result[key] = value
    if set(result) != set(fields): raise ValueError('missing or unexpected timing fields')
    return result


def integer_fields(record, fields):
    for field in fields:
        value = record[field]
        if not value.isascii() or not value.isdecimal(): raise ValueError('invalid nonnegative timing integer')
        record[field] = int(value)
    return record


def wall_limit(seconds, units, rounding=0):
    if type(seconds) not in (int, float) or not math.isfinite(seconds) or seconds <= 0:
        raise ValueError('invalid request wall time')
    return seconds * units + rounding


def generation_timing(text, wall_seconds):
    fields = GENERATION_PHASES + ('total',)
    row = integer_fields(one_record(text, 'generate_timing_ms: ', fields), fields)
    if row['total'] != sum(row[k] for k in GENERATION_PHASES): raise ValueError('generation phases do not reconcile')
    if not 0 < row['total'] <= wall_limit(wall_seconds, 1000, rounding=2):
        raise ValueError('generation timing exceeds request wall interval')
    return row


def prepare_timing(text, logical_tokens, checkpoint_bytes, wall_seconds, outcome='compacted'):
    integers = ('source_tokens',) + PREPARE_PHASES + ('export_clock_valid', 'checkpoint_bytes', 'terminal_complete')
    row = integer_fields(one_record(text, 'native_checkpoint_prepare_work ', integers + ('outcome',)), integers)
    if (row['source_tokens'] != logical_tokens or row['checkpoint_bytes'] != checkpoint_bytes or
            row['outcome'] != outcome or row['terminal_complete'] != 0):
        raise ValueError('preparation timing belongs to a different export')
    if row['export_clock_valid'] != 1 or any(row[k] <= 0 for k in PREPARE_PHASES):
        raise ValueError('missing preparation phase timing')
    total = sum(row[k] for k in PREPARE_PHASES)
    if total > wall_limit(wall_seconds, 1e9): raise ValueError('preparation phases exceed request wall interval')
    row['measured_work_sum_ns'] = total
    return row


def request_log(path, offset):
    with path.open('rb') as source:
        if source.seek(0, 2) < offset: raise ValueError('worker log truncated during request')
        source.seek(offset)
        return source.read().decode('utf-8')
