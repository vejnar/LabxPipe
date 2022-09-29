#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# Copyright (C) 2013-2022 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

"""Run project"""

import argparse
import concurrent.futures
import datetime
import json
import os
import shutil
import subprocess
import sys
import threading

import labxdb

import pyfnutils as pfu
import pyfnutils.log

import labxpipe.steps

def start_pipeline(run_cmd, path_pipeline, num_processor, run_ref, replicate_ref, http_url, http_login, http_password, http_path, http_db, failing):
    if failing.is_set() == False:
        try:
            cmd = run_cmd + ['--pipeline', path_pipeline, '--processor', str(num_processor)]
            if run_ref is not None:
                cmd.extend(['--run', run_ref])
            if replicate_ref is not None:
                cmd.extend(['--replicate', replicate_ref])
            if http_url is not None:
                cmd.extend(['--http_url', http_url])
            if http_login is not None:
                cmd.extend(['--http_login', http_login])
            if http_password is not None:
                cmd.extend(['--http_password', http_password])
            if http_path is not None:
                cmd.extend(['--http_path', http_path])
            if http_db is not None:
                cmd.extend(['--http_db', http_db])
            p = subprocess.run(cmd, check=True)
        except:
            failing.set()
            raise

def clean_stop(completion, completion_fname, logger):
    logger.info('Saving completion state')
    json.dump(completion, open(completion_fname, 'w'), sort_keys=True, indent=4, separators=(',', ': '))

def now():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def main(argv=None):
    if argv is None:
        argv = sys.argv
    # Started from wrapper?
    prog = os.path.basename(argv[0])
    if argv[1] == 'run':
        job_cmd = argv[:2]
        argv_parser = argv[2:]
        prog += ' run'
    else:
        job_cmd = argv[:1]
        argv_parser = argv[1:]
    # Parse arguments
    parser = argparse.ArgumentParser(prog=prog, description='Analyze sequencing expt.')
    parser.add_argument('-c', '--pipeline', dest='path_pipeline', action='store', required=True, help='Path to pipeline')
    parser.add_argument('-r', '--run', dest='run_ref', action='store', help='Run')
    parser.add_argument('-n', '--replicate', dest='replicate_ref', action='store', help='Replicate')
    parser.add_argument('-w', '--worker', dest='num_worker', action='store', type=int, default=1, help='Number of run in parallel')
    parser.add_argument('-p', '--processor', dest='num_processor', action='store', type=int, default=2, help='Number of processor per run')
    parser.add_argument('--path_config', dest='path_config', action='store', help='Path to config')
    parser.add_argument('--http_url', '--labxdb_http_url', dest='labxdb_http_url', action='store', help='Database HTTP URL')
    parser.add_argument('--http_login', '--labxdb_http_login', dest='labxdb_http_login', action='store', help='Database HTTP login')
    parser.add_argument('--http_password', '--labxdb_http_password', dest='labxdb_http_password', action='store', help='Database HTTP password')
    parser.add_argument('--http_path', '--labxdb_http_path', dest='labxdb_http_path', action='store', help='Database HTTP path')
    parser.add_argument('--http_db', '--labxdb_http_db', dest='labxdb_http_db', action='store', help='Database HTTP DB')
    args = parser.parse_args(argv_parser)

    # Logging is not yet available: temporary saving messages
    to_log = []
    
    # Load config: Global (JSON single file or all files in path_config)
    config = {}
    paths = []
    if args.path_config is None:
        if 'HTS_CONFIG_PATH' in os.environ:
            paths.append(os.environ['HTS_CONFIG_PATH'])
        elif 'XDG_CONFIG_HOME' in os.environ:
            paths.append(os.path.join(os.environ['XDG_CONFIG_HOME'], 'hts'))
    else:
        paths.append(args.path_config)
    for path in paths:
        if os.path.isdir(path):
            for f in sorted(os.listdir(path)):
                if f.endswith('.json'):
                    to_log.append(f'Load global config ({f})')
                    config = {**config, **json.load(open(os.path.join(path, f)))}
        elif os.path.isfile(path):
            to_log.append(f'Load global config ({path})')
            config = {**config, **json.load(open(path))}

    # Input local config from args
    vargs = vars(args)
    for a, v in vargs.items():
        if v is not None and (a not in config or v != parser.get_default(a)):
            config[a] = v

    # LabxDB parameters
    if 'labxdb_http_path' not in config and 'labxdb_http_db' not in config:
        if 'labxdb_http_path_seq' in config:
            config['labxdb_http_path'] = config['labxdb_http_path_seq']
        else:
            config['labxdb_http_db'] = 'seq'

    # Load config: Project
    if not os.path.exists(config['path_pipeline']):
        print('ERROR: Pipeline file not found')
        return 1
    else:
        to_log.append(f"Load project config ({os.path.abspath(config['path_pipeline'])})")
        config = {**config, **json.load(open(config['path_pipeline']))}

    # Start all runs
    if 'run_ref' not in config and 'replicate_ref' not in config:
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=config['num_worker']) as executor:
                # Failing event (with FIRST_EXCEPTION, the next job starts before remaining jobs get cancelled)
                failing = threading.Event()
                # Prepare jobs
                jobs = []
                is_force = any([s['force'] for s in config['analysis']])
                refs = []
                if 'run_refs' in config:
                    refs.extend([(i, None, i) for i in config['run_refs']])
                if 'replicate_refs' in config:
                    refs.extend([(None, i, i) for i in config['replicate_refs']])
                for run_ref, replicate_ref, seq_ref in refs:
                    path_json_compl = os.path.join(config['path_output'], seq_ref, 'log', config['name']+'_compl.json')
                    if is_force or not os.path.exists(path_json_compl):
                        jobs.append([job_cmd, config['path_pipeline'], config['num_processor'], run_ref, replicate_ref, config.get('labxdb_http_url'), config.get('labxdb_http_login'), config.get('labxdb_http_password'), config.get('labxdb_http_path'), config.get('labxdb_http_db'), failing])
                    elif os.path.exists(path_json_compl):
                        ncompl = len([s for s in json.load(open(path_json_compl)) if s['status'] == 'done'])
                        if len(config['analysis']) > ncompl:
                            jobs.append([job_cmd, config['path_pipeline'], config['num_processor'], run_ref, replicate_ref, config.get('labxdb_http_url'), config.get('labxdb_http_login'), config.get('labxdb_http_password'), config.get('labxdb_http_path'), config.get('labxdb_http_db'), failing])
                # Add jobs to queue
                fs = []
                for job in jobs:
                    fs.append(executor.submit(start_pipeline, *job))
                # Wait
                try:
                    rfs = concurrent.futures.wait(fs, return_when=concurrent.futures.FIRST_EXCEPTION)
                except KeyboardInterrupt:
                    for j in fs:
                        j.cancel()
                    executor.shutdown()
                else:
                    for t in rfs.not_done:
                        t.cancel()
                    for t in rfs.done:
                        t.result()
        except Exception as e:
            print(e)
            sys.exit(1)

    # Single run/replicate
    else:
        # Analysis name
        if 'run_ref' in config:
            config['seq_ref'] = config['run_ref']
        elif 'replicate_ref' in config:
            config['seq_ref'] = config['replicate_ref']
        # Root directory for analysis
        path_analysis = os.path.join(config['path_output'], config['seq_ref'])
        config['path_analysis'] = path_analysis
        if not os.path.exists(path_analysis):
            os.makedirs(path_analysis)
        path_log = os.path.join(path_analysis, 'log')
        if not os.path.exists(path_log):
            os.makedirs(path_log)

        # Start logging
        logger_name = 'Analysis_' + config['seq_ref']
        logger = pfu.log.define_root_logger(logger_name, level=config['logging_level'], filename=os.path.join(path_log, 'all.log'), log_uncaught=True)
        config['logger_name'] = logger_name

        # Logging buffered log lines
        logger.info('Starting')
        for line in to_log:
            logger.info(line)

        # Load available run functions
        run_functions = {}
        for name in labxpipe.steps.__all__:
            step_mod = getattr(labxpipe.steps, name)
            for n in getattr(step_mod, 'functions'):
                run_functions[n] = step_mod.run

        # Completion object
        completion_fname = os.path.join(path_log, config['name']+'_compl.json')
        if os.path.exists(completion_fname):
            logger.info('Loading project completion')
            completion = json.load(open(completion_fname))
        else:
            completion = []
        # Add missing steps in completion object
        for op in config['analysis']:
            if op['step_name'] not in [op['step_name'] for op in completion]:
                completion.append({'step_name':op['step_name'], 'start':None, 'end':None, 'status':None})

        # Get run info
        runs = []
        if 'db' in config['ref_info_source']:
            # Init. DBLink
            dbl = labxdb.DBLink(config.get('labxdb_http_url'), config.get('labxdb_http_login'), config.get('labxdb_http_password'), config.get('labxdb_http_path'), config.get('labxdb_http_db'))
            if 'run_ref' in config:
                # Query: Run
                run = dbl.get('run/get-ref/'+config['run_ref'])[0][0]
            elif 'replicate_ref' in config:
                # Query: Get all run(s)
                runs = dbl.post('run', {'search_criterion':['3 replicate_ref EQUAL '+config['replicate_ref']], 'sort_criterion':['3 run_order ASC'], 'limit':'ALL'})
                # First run as reference run
                run = runs[0]
            # Copy-paste info to config
            for field in ['quality_scores', 'directional', 'paired', 'r1_strand', 'max_read_length']:
                config[field] = run[field]
            # Query: Replicate
            replicate = dbl.get('replicate/get-ref/'+run['replicate_ref'])[0][0]
            # Copy-paste info to config
            config['label_short'] = replicate['label_short']
            # Query: Sample
            sample = dbl.get('sample/get-ref/'+replicate['sample_ref'])[0][0]
            # Get adapter sequence
            if sample['adapter_3p'] is not None and sample['adapter_3p'] in config['adaptors']:
                config['adaptor_3p'] = config['adaptors'][sample['adapter_3p']]
            if sample['adapter_5p'] is not None and sample['adapter_5p'] in config['adaptors']:
                config['adaptor_5p'] = config['adaptors'][sample['adapter_5p']]
        if 'json' in config['ref_info_source'] and config['seq_ref'] in config['ref_infos']:
            config.update(config['ref_infos'][config['seq_ref']])

        try:
            if len(runs) > 0:
                path_input =  os.path.join(path_analysis, 'input')
                if not os.path.exists(path_input):
                    os.makedirs(path_input)
                for run in runs:
                    path_target = os.path.join(path_input, run['run_ref'])
                    if not os.path.exists(path_target):
                        # If one link is missing, force to run the analysis
                        logger.info(f"New input found: {run['run_ref']} - Forcing to run all steps")
                        for op in config['analysis']:
                            op['force'] = True
                        # Create link
                        path_data = os.path.join(config['path_seq_run'], run['run_ref'])
                        if os.path.exists(path_data):
                            os.symlink(path_data, path_target)
                        else:
                            raise FileNotFoundError(f'Input {path_data} not found')
            else:
                path_input =  os.path.join(config['path_seq_run'], config['run_ref'])
            name_input = 'Input:' + config['seq_ref']
            nstep = len(config['analysis'])
            for iop, op in enumerate(config['analysis']):
                # Output dir.
                path_output = os.path.join(path_analysis, op['step_name'])
                if completion[iop]['end'] is None or op['force']:
                    logger.info(f"Start {op['step_name']} - Input step {name_input}")
                    completion[iop]['end'] = None
                    completion[iop]['status'] = None
                    # Output dir.
                    if os.path.exists(path_output):
                        shutil.rmtree(path_output)
                    os.mkdir(path_output)
                    # Log start time
                    completion[iop]['start'] = now()

                    # Do the job
                    config_op = {**config, **op}
                    if 'subpath_input' in config_op:
                        path_input = os.path.join(path_input, config_op['subpath_input'])
                    if 'step_function' in op:
                        fn_step = run_functions[op['step_function']]
                    else:
                        fn_step = run_functions[op['step_name']]
                    fn_step(path_input, path_output, config_op)

                    # Log end time
                    completion[iop]['end'] = now()
                    completion[iop]['status'] = 'done'
                    logger.info(f"End {op['step_name']}")

                # Determine path_input for next step
                if iop < nstep - 1:
                    next_op = config['analysis'][iop+1]
                    if 'step_input' in next_op:
                        for tmp_op in config['analysis']:
                            if tmp_op['step_name'] == next_op['step_input']:
                                path_input = os.path.join(path_analysis, tmp_op['step_name'])
                                name_input = tmp_op['step_name']
                                break
                    else:
                        path_input = path_output
                        name_input = op['step_name']
        except KeyboardInterrupt:
            clean_stop(completion, completion_fname, logger)
        except:
            clean_stop(completion, completion_fname, logger)
            raise
        else:
            clean_stop(completion, completion_fname, logger)

if __name__ == '__main__':
    sys.exit(main())
