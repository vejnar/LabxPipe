#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# Copyright (C) 2013-2022 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

"""Demultiplex sequencing reads"""

import argparse
import copy
import json
import logging
import os
import shutil
import subprocess
import sys

import labxdb
import labxdb.fastq

import pyfnutils as pfu
import pyfnutils.log

import labxpipe.interfaces.if_exe_readknead

class Error(Exception):
    def __init__(self, message):
        self.message = message

def demultiplex(bulk, path_demux_ops, path_seq_tmp, path_seq_raw, path_seq_prepared, input_run_refs=[], exclude_run_refs=[], demux_nozip=False, demux_verbose_level=2, fastq_exts=['.fastq'], no_readonly=False, dry_run=False, dbl=None, config=None, num_processor=1, logger=None):
    # Parameters
    if logger is None:
        import logging as logger

    logger.info('Demultiplexing runs')

    # List FASTQ files
    fastqs = labxdb.fastq.find_fastqs(os.path.join(path_seq_raw, bulk), fastq_exts, [labxdb.fastq.parse_illumina_fastq_filename, labxdb.fastq.parse_fastq_filename])
    if labxdb.fastq.check_fastqs(fastqs) is False:
        raise Error('Name collision: Runs with the same name detected in different folder')

    # Open demultiplex operations
    if os.path.isdir(path_demux_ops):
        dmx_ops = {}
        for p in os.listdir(path_demux_ops):
            dmx_ops |= json.load(open(p))
    else:
        dmx_ops = json.load(open(path_demux_ops))

    # Create output folder
    if path_seq_tmp is not None:
        path_bulk_output = os.path.join(path_seq_tmp, bulk)
    else:
        path_bulk_output = os.path.join(path_seq_prepared, bulk)
    if not os.path.exists(path_bulk_output):
        if dry_run:
            logger.info(f'DRY RUN: os.makedir({path_bulk_output})')
        else:
            os.mkdir(path_bulk_output)

    # Demultiplex
    for name, path_list in fastqs.items():
        # Get run info
        if dbl:
            search_criterion = []
            # Flowcell
            flowcells = set()
            for p in path_list:
                r = labxdb.fastq.get_illumina_fastq_info(os.path.join(p['path'], p['fname']))
                flowcells.add(r['flowcell'])
            if len(flowcells) == 1:
                flowcell = list(flowcells)[0]
                search_criterion.append('3 flowcell FUZZY '+flowcell)
            else:
                raise Error('Multiple flowcell detected in a single run')
            # Tube label
            search_criterion.append('3 tube_label EQUAL '+name)
            # Query: Run
            runs = dbl.post('run', {'search_criterion':search_criterion, 'search_gate':'AND', 'limit':'ALL'})
            if len(input_run_refs) > 0:
                runs = [r for r in runs if r['run_ref'] in input_run_refs]
            if len(exclude_run_refs) > 0:
                runs = [r for r in runs if r['run_ref'] not in exclude_run_refs]
            if len(runs) == 0:
                logger.warning(f'{name} had no run')
                continue
            # Get info from first run
            quality_scores = runs[0]['quality_scores']
            max_read_length = runs[0]['max_read_length']
            barcode = runs[0]['barcode']
            # Barcodes
            second_barcodes = [r['second_barcode'] for r in runs if r['second_barcode'] is not None]
            if len(second_barcodes) == 0:
                logger.warning(f'{name} had no second_barcode')
                continue
            # Get adapter's name (from the sample of the first run as all samples have identical adapters since they are demultiplexed from the same run)
            replicate = dbl.get('replicate/get-ref/'+runs[0]['replicate_ref'])
            sample = dbl.get('sample/get-ref/'+replicate[0][0]['sample_ref'])
            adapter_3p = sample[0][0]['adapter_3p']

        # Input paths
        input_r1_paths = [os.path.join(p['path'], p['fname']) for p in path_list if p['end'] == 'R1']
        input_r2_paths = [os.path.join(p['path'], p['fname']) for p in path_list if p['end'] == 'R2']
        if len(input_r1_paths) == 0:
            input_r1_paths = None
        if len(input_r2_paths) == 0:
            input_r2_paths = None
        # Output path template
        output_tpl = []
        for r in ['R1', 'R2']:
            ots = [p['fname'] for p in path_list if p['end'] == r]
            if len(ots) > 0:
                ot = ots[0].replace('.gz', '').replace('.zst', '')
                if ot.find('_'+r) != -1:
                    x = ot.find('_'+r)
                    ot = ot[:x] + '-[DPX]' + ot[x:]
                elif ot.find(barcode) != -1:
                    x = ot.find(barcode)
                    ot = ot[:x] + '-[DPX]' + ot[x:]
                else:
                    ot += '-[DPX]'
            else:
                ot = None
            output_tpl.append(ot)
        # Prepare parameters
        if adapter_3p in dmx_ops:
            dmx_op = copy.deepcopy(dmx_ops[adapter_3p])
            for end in dmx_op.keys():
                if dmx_op[end] is not None:
                    for i in range(len(dmx_op[end])):
                        if dmx_op[end][i]['name'] == 'trim' and 'sequence' not in dmx_op[end][i] and 'sequences' not in dmx_op[end][i]:
                            dmx_op[end][i]['sequence'] = config['adaptors'][adapter_3p]
                        elif dmx_op[end][i]['name'] == 'demultiplex' and 'barcodes' not in dmx_op[end][i]:
                            dmx_op[end][i]['barcodes'] = second_barcodes
        else:
            raise Error(f'"{adapter_3p}" not found in demultiplex operations')

        # Start ReadKnead
        if dry_run:
            logger.info(f"DRY RUN: ReadKnead({input_r1_paths}, {input_r2_paths}, {path_bulk_output}, {quality_scores}, {dmx_op}, {os.path.join(path_bulk_output, f'preparing_report_{name}.json')}, {max_read_length})")
        else:
            labxpipe.interfaces.if_exe_readknead.readknead(input_r1_paths,
                                                           input_r2_paths,
                                                           outpath              = path_bulk_output,
                                                           fq_fname_out_r1      = output_tpl[0],
                                                           fq_fname_out_r2      = output_tpl[1],
                                                           quality_score        = quality_scores,
                                                           ops_r1               = dmx_op['R1'],
                                                           ops_r2               = dmx_op['R2'],
                                                           report_path          = os.path.join(path_bulk_output, f'preparing_report_{name}.json'),
                                                           stats_out_path       = os.path.join(path_bulk_output, f'stats_out_{name}'),
                                                           max_read_length      = max_read_length,
                                                           num_worker           = num_processor,
                                                           verbose              = True,
                                                           verbose_level        = demux_verbose_level,
                                                           return_std           = False,
                                                           logger               = logger)
            # Zip FASTQ
            if demux_nozip is False:
                for p in output_tpl:
                    if p is not None:
                        for b in second_barcodes + ['undetermined']:
                            cmd = ['zstd', '--rm', '-T'+str(num_processor), '-19', os.path.join(path_bulk_output, p.replace('[DPX]', b))]
                            logger.info(cmd)
                            subprocess.run(cmd, check=True)

    if path_seq_tmp is not None:
        if dry_run:
            logger.info(f'DRY RUN: Move {path_bulk_output} to {path_seq_prepared}')
        else:
            logger.info(f'Move {path_bulk_output} to {path_seq_prepared}')
            shutil.move(path_bulk_output, path_seq_prepared)
            # Update output path after moving
            path_bulk_output = os.path.join(path_seq_prepared, bulk)
    if not dry_run and no_readonly is False:
        # Change permissions to read-only
        for f in os.listdir(path_bulk_output):
            os.chmod(os.path.join(path_bulk_output, f), 0o0444)
        os.chmod(path_bulk_output, 0o0555)

def check_exe(names):
    for name in names:
        if shutil.which(name) == None:
            raise Error(f'{name} missing')

def get_first_key(l, d):
    for k in l:
        if k in d:
            return d[k]

def main(argv=None):
    if argv is None:
        argv = sys.argv
    # Started from wrapper?
    prog = os.path.basename(argv[0])
    if argv[1] == 'demultiplex':
        job_cmd = argv[:2]
        argv_parser = argv[2:]
        prog += ' demultiplex'
    else:
        job_cmd = argv[:1]
        argv_parser = argv[1:]
    # Parse arguments
    parser = argparse.ArgumentParser(prog=prog, description='Demultiplex sequencing reads.')
    # Main
    group = parser.add_argument_group('Main')
    group.add_argument('-b', '--bulk', dest='bulk', action='store', help='Sequencing bulk name')
    group.add_argument('-r', '--path_seq_raw', dest='path_seq_raw', action='store', help='Path to raw seq.')
    group.add_argument('-a', '--path_seq_prepared', dest='path_seq_prepared', action='store', help='Path to prepared.')
    group.add_argument('-n', '--path_seq_tmp', dest='path_seq_tmp', action='store', help='Path to temporary.')
    group.add_argument('-k', '--no_readonly', dest='no_readonly', action='store_true', help='No read-only chmod')
    group.add_argument('-d', '--dry_run', dest='dry_run', action='store_true', help='Dry run')
    group.add_argument('-p', '--processor', dest='num_processor', action='store', type=int, default=6, help='Number of processor')
    group.add_argument('--path_config', dest='path_config', action='store', help='Path to config')
    group.add_argument('--fastq_exts', dest='fastq_exts', action='store', default='.fastq,.fastq.gz,.fastq.zst', help='FASTQ file extensions (comma separated).')
    group.add_argument('--http_url', '--labxdb_http_url', dest='labxdb_http_url', action='store', help='Database HTTP URL')
    group.add_argument('--http_login', '--labxdb_http_login', dest='labxdb_http_login', action='store', help='Database HTTP login')
    group.add_argument('--http_password', '--labxdb_http_password', dest='labxdb_http_password', action='store', help='Database HTTP password')
    group.add_argument('--http_path', '--labxdb_http_path', dest='labxdb_http_path', action='store', help='Database HTTP path')
    group.add_argument('--http_db', '--labxdb_http_db', dest='labxdb_http_db', action='store', help='Database HTTP DB')
    # Demultiplex
    group = parser.add_argument_group('Demultiplex')
    group.add_argument('-u', '--path_demux_ops', dest='path_demux_ops', action='store', help='Path to file(s) configuring demultiplex operations')
    group.add_argument('--demux_nozip', dest='demux_nozip', action='store_true', help='Skip compressing demultiplexed output')
    group.add_argument('--demux_verbose_level', dest='demux_verbose_level', action='store', default='2', help='Demultiplexing verbose level')
    group.add_argument('-t', '--input_run_refs', dest='input_run_refs', action='store', help='Only import these runs (comma separated references)')
    group.add_argument('--exclude_run_refs', dest='exclude_run_refs', action='store', help='Exclude these runs from import (comma separated references)')
    args = parser.parse_args(argv_parser)

    # Get config (JSON single file or all files in path_config)
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
                    config = {**config, **json.load(open(os.path.join(path, f)))}
        elif os.path.isfile(path):
            config = {**config, **json.load(open(path))}

    # Input local config from args
    vargs = vars(args)
    for a, v in vargs.items():
        if v is not None and (a not in config or v != parser.get_default(a)):
            config[a] = v
        if a in ['fastq_exts', 'input_run_refs', 'exclude_run_refs']:
            if v is None:
                config[a] = []
            else:
                config[a] = [r.strip() for r in v.split(',')]

    # Check options
    if 'bulk' not in config:
        print('ERROR: --bulk is required')
        return 1
    if 'path_seq_raw' not in config:
        print('ERROR: --path_seq_raw is required')
        return 1
    # Check folders
    if not os.path.exists(config['path_seq_raw']):
        print(f"ERROR: {config['path_seq_raw']} not found")
        return 1
    if 'path_seq_tmp' in config and not os.path.exists(config['path_seq_tmp']):
        print(f"ERROR: {config['path_seq_tmp']} not found")
        return 1
    if 'path_seq_prepared' in config and not os.path.exists(config['path_seq_prepared']):
        print(f"ERROR: {config['path_seq_prepared']} not found")
        return 1
    # Check software
    check_exe(['readknead'])
    if config['demux_nozip'] is False:
        check_exe(['zstd'])

    # Init. demultiplex
    if not config['dry_run']:
        path_demultiplex = os.path.join(get_first_key(['path_seq_tmp', 'path_seq_prepared'], config), config['bulk'])
        if not os.path.exists(path_demultiplex):
            os.mkdir(path_demultiplex)
        log_filename = os.path.join(path_demultiplex, 'demultiplex.log')
    else:
        log_filename = None

    # Logging
    logger = pfu.log.define_root_logger(f"load_{config['bulk']}", level='info', filename=log_filename)
    logger.info('Starting')

    try:
        # LabxDB
        if 'labxdb_http_path' not in config and 'labxdb_http_db' not in config:
            if 'labxdb_http_path_seq' in config:
                config['labxdb_http_path'] = config['labxdb_http_path_seq']
            else:
                config['labxdb_http_db'] = 'seq'
        dbl = labxdb.DBLink(config.get('labxdb_http_url'), config.get('labxdb_http_login'), config.get('labxdb_http_password'), config.get('labxdb_http_path'), config.get('labxdb_http_db'))
        # Demultiplex
        demultiplex(config['bulk'], config['path_demux_ops'], config.get('path_seq_tmp'), config['path_seq_raw'], config['path_seq_prepared'], config['input_run_refs'], config['exclude_run_refs'], config['demux_nozip'], config['demux_verbose_level'], fastq_exts=config['fastq_exts'], no_readonly=config['no_readonly'], dry_run=config['dry_run'], dbl=dbl, config=config, num_processor=config['num_processor'], logger=logger)
    except Error as e:
        logger.error(e.message)
        return 1

if __name__ == '__main__':
    sys.exit(main())
