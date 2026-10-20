"""Evaluate the documented latency ratios for one matched full-replay workload.

This is a scoped generation comparison, not production qualification. Portable
restore and resident prefix reuse cannot be substituted for full prompt replay.
"""
import argparse
import hashlib
import json
from pathlib import Path

from qualify_matched_llama import percentiles
from native_benchmark_fixture import LOGICAL_TOKENS,MODEL_IDENTITY


def measured_rows(report):
    if report.get('passed') is not True or report.get('sampling_qualified') is not True or report.get('smoke') is not False:
        raise ValueError('passing non-smoke sampled campaign required')
    if report.get('mode')!='replay':raise ValueError('only full prompt replay is comparable here')
    warmups=report['warmups_requested'];samples=report['samples_requested']
    if type(warmups) is not int or type(samples) is not int or warmups<5 or samples<100:
        raise ValueError('insufficient declared sampling')
    rows=report['observations']
    if len(rows)!=1+warmups+samples:raise ValueError('incomplete observation count')
    for index,row in enumerate(rows):
        if (type(row.get('index')) is not int or row['index']!=index or row.get('passed') is not True or
                row.get('cold') is not (index==0) or row.get('warmup') is not (0<index<=warmups)):
            raise ValueError('invalid observation sequence or outcome')
        percentiles([row['total_seconds'],row['first_content_seconds']])
        if row['first_content_seconds']>row['total_seconds']:raise ValueError('invalid first-content timing')
    return rows[1+warmups:]


def compare(native,llama):
    a=measured_rows(native);b=measured_rows(llama)
    size=native['size']
    if size not in LOGICAL_TOKENS or llama['size']!=size:raise ValueError('different workload sizes')
    logical=LOGICAL_TOKENS[size]
    for field in ('identity','logical_tokens','checkpoint_sha256_to_reverify','token_bytes_sha256'):
        if native['fixture'][field]!=llama['fixture'][field]:raise ValueError(f'fixture mismatch: {field}')
    if native['fixture']['logical_tokens']!=logical or native['fixture']['identity']!=MODEL_IDENTITY:
        raise ValueError('unexpected designated model/token fixture')
    native_models=[digest for name,digest in native['input_sha256'].items() if name.endswith('.gguf')]
    if native_models!=[llama['model_sha256']]:raise ValueError('GGUF revision mismatch')
    if native['hardware']!=llama['hardware'] or len(native['hardware'])!=1 or native['hardware'][0]['name']!='NVIDIA L4':
        raise ValueError('hardware/driver mismatch')
    request=native['request'];settings=llama['request_settings']
    if (request.get('temperature')!=0 or settings.get('temperature')!=0 or
            request.get('max_tokens')!=settings.get('n_predict') or request.get('max_tokens')!=16 or
            request.get('prompt_cache') is not False or settings.get('cache_prompt') is not False or
            request.get('cache_dtype')!='f16' or settings.get('repeat_penalty')!=1):
        raise ValueError('generation settings mismatch')
    if native['worker'].get('server_kind')!='standalone' or native['worker'].get('stopped') is not True or llama.get('process_group_stopped') is not True:
        raise ValueError('serving scope or cleanup unverified')
    # Verify f16 KV for llama from the actual recorded launch, not a label.
    command=llama['command']
    for flag in ('-ctk','-ctv'):
        if command.count(flag)!=1 or command[command.index(flag)+1]!='f16':raise ValueError('KV precision mismatch')
    expected_count=None;expected_text=None
    for label,report in [('antfly',native),('llama',llama)]:
        for row in report['observations']:
            result=row['result']
            if label=='antfly':
                usage=result['usage'];choices=result['choices']
                if len(choices)!=1 or choices[0]['finish_reason']!='stop':raise ValueError('unexpected Antfly termination')
                count=usage['completion_tokens'];text=choices[0]['message']['content'].strip()
                if usage['prompt_tokens']!=logical or usage.get('cached_prompt_tokens',0)!=0:
                    raise ValueError('Antfly replay reused or changed the prompt')
            else:
                timing=result['timings'];count=result['tokens_predicted'];text=result['content'].strip()
                if (result['tokens_evaluated']!=logical or timing['prompt_n']!=logical or timing['cache_n']!=0 or
                        timing['predicted_n']!=count or result['stop_type']!='eos' or result['truncated'] is not False):
                    raise ValueError('llama replay reused, truncated or changed the workload')
            if type(count) is not int or not 0<count<=16 or text!='violet-729':raise ValueError('invalid generated answer/count')
            if expected_count is None:expected_count=count;expected_text=text
            if (count,text)!=(expected_count,expected_text):raise ValueError('output lengths or answers differ')
    summaries={label:{key:percentiles([r[key] for r in rows]) for key in ('total_seconds','first_content_seconds')}
               for label,rows in [('antfly',a),('llama',b)]}
    ratios={metric:{q:summaries['antfly'][metric][q]/summaries['llama'][metric][q] for q in ('p50','p95')}
            for metric in ('total_seconds','first_content_seconds')}
    limits={'p50':1.25,'p95':1.5}
    return {'scope':'One designated full-prompt-replay workload; total wall latency gate, first-content ratios reported separately',
            'size':size,'logical_tokens':logical,'output_tokens':expected_count,'comparable':True,
            'samples':{'antfly':len(a),'llama':len(b)},'latency_seconds':summaries,'antfly_over_llama':ratios,
            'total_latency_ratio_limits':limits,
            'latency_thresholds_met':all(ratios['total_seconds'][q]<=limit for q,limit in limits.items()),
            'production_ready':False,'portable_restore_comparison_qualified':False}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--antfly',type=Path,required=True)
    parser.add_argument('--llama',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.output.exists():raise FileExistsError(args.output)
    result={'comparable':False,'latency_thresholds_met':False,'production_ready':False}
    try:
        raw_a=args.antfly.read_bytes();raw_b=args.llama.read_bytes()
        result.update(compare(json.loads(raw_a),json.loads(raw_b)))
        result['input_sha256']={str(args.antfly):hashlib.sha256(raw_a).hexdigest(),str(args.llama):hashlib.sha256(raw_b).hexdigest()}
    except Exception as error:result['error']=f'{type(error).__name__}: {error}'
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with args.output.open('x') as output:output.write(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result),flush=True)
    return 0 if result['comparable'] and result['latency_thresholds_met'] else 1


if __name__=='__main__':raise SystemExit(main())
