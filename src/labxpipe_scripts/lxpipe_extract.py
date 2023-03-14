#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# Copyright © 2023 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

"""Extract & rename project output files"""

import argparse
import json
import os
import sys

import labxdb

from labxpipe import utils


def main(argv=None):
    if argv is None:
        argv = sys.argv
    # Started from wrapper?
    prog = os.path.basename(argv[0])
    if len(argv) > 1 and argv[1] == 'extract':
        job_cmd = argv[:2]
        argv_parser = argv[2:]
        prog += ' extract'
    else:
        job_cmd = argv[:1]
        argv_parser = argv[1:]
    # Parse arguments
    parser = argparse.ArgumentParser(prog=prog, description='Extract & rename project output files.')
    parser.add_argument('-c', '--pipeline', dest='path_pipeline', action='store', required=True, help='Path to pipeline')
    parser.add_argument('-f', '--files', dest='files', action='store', required=True, help='Files to extract (comma separated list of step,filename pairs)')
    parser.add_argument('-o', '--path_extract', dest='path_extract', action='store', default='.', help='Path to output')
    parser.add_argument('-l', '--label', dest='use_label', action='store_true', help='Use label to rename')
    parser.add_argument('-r', '--reference', dest='use_reference', action='store_true', help='Use reference to rename')
    parser.add_argument('-w', '--lowercase', dest='lowercase', action='store_true', help='Lowercase filename')
    parser.add_argument('-e', '--extensions', dest='search_extensions', action='store', help='File extensions to search (comma separated)')
    parser.add_argument('-d', '--dry_run', dest='dry_run', action='store_true', help='Dry run')
    parser.add_argument('--path_config', dest='path_config', action='store', help='Path to config')
    parser.add_argument('--http_url', '--labxdb_http_url', dest='labxdb_http_url', action='store', help='Database HTTP URL')
    parser.add_argument('--http_login', '--labxdb_http_login', dest='labxdb_http_login', action='store', help='Database HTTP login')
    parser.add_argument('--http_password', '--labxdb_http_password', dest='labxdb_http_password', action='store', help='Database HTTP password')
    parser.add_argument('--http_path', '--labxdb_http_path', dest='labxdb_http_path', action='store', help='Database HTTP path')
    parser.add_argument('--http_db', '--labxdb_http_db', dest='labxdb_http_db', action='store', help='Database HTTP DB')
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
        if a in ['search_extensions']:
            if v is None:
                config[a] = []
            else:
                config[a] = [r.strip() for r in v.split(',')]

    # Load config: Project
    if not os.path.exists(config['path_pipeline']):
        print('ERROR: Pipeline file not found')
        return 1
    else:
        config = {**config, **json.load(open(config['path_pipeline']))}

    # Parse files argument
    raw_files = args.files.split(',')
    assert len(raw_files) % 2 == 0, 'ERROR: --files must be a list of pairs step,filename comma separated'
    # Split files into pairs
    files = [(raw_files[i], raw_files[i + 1]) for i in range(0, len(raw_files), 2)]

    # Default rename uses label
    if args.use_label is False and args.use_reference is False:
        args.use_label = True

    # Get info
    if 'db' in config['ref_info_source']:
        # Init. DBLink
        if 'labxdb_http_path' not in config and 'labxdb_http_db' not in config:
            if 'labxdb_http_path_seq' in config:
                config['labxdb_http_path'] = config['labxdb_http_path_seq']
            else:
                config['labxdb_http_db'] = 'seq'
        dbl = labxdb.DBLink(config.get('labxdb_http_url'), config.get('labxdb_http_login'), config.get('labxdb_http_password'), config.get('labxdb_http_path'), config.get('labxdb_http_db'))

        # Create ref_infos
        config['ref_infos'] = {}

        # Get info from run level
        if 'run_refs' in config:
            for run_ref in config['run_refs']:
                run = dbl.get('run/get-ref/' + run_ref)[0][0]
                replicate = dbl.get('replicate/get-ref/' + run['replicate_ref'])[0][0]
                config['ref_infos'][run_ref] = {'label_short': replicate['label_short']}
        # Get info from replicate level
        if 'replicate_refs' in config:
            for replicate_ref in config['replicate_refs']:
                replicate = dbl.get('replicate/get-ref/' + replicate_ref)[0][0]
                config['ref_infos'][replicate_ref] = {'label_short': replicate['label_short']}

    # Create move list
    moves = []
    for ref in config.get('run_refs', 'replicate_refs'):
        for step, fname in files:
            path_in = os.path.join(config['path_output'], ref, step, fname)
            if os.path.exists(path_in):
                fname_ext = None
                for ext in utils.all_exts + config['search_extensions']:
                    if fname.rfind(ext) != -1:
                        fname_ext = fname[fname.rfind(ext) :]
                        break
                if fname_ext is None:
                    fname_ext = fname[fname.rfind('.') :]
                if fname_ext is None:
                    fname_ext = fname
                if args.use_label:
                    label = utils.label2var(config['ref_infos'][ref]['label_short'])
                if args.use_label and args.use_reference:
                    name = f'{label}_{ref}'
                elif args.use_label:
                    name = label
                elif args.use_reference:
                    name = ref
                if args.lowercase:
                    name = name.lower()
                path_out = os.path.join(config['path_extract'], name + fname_ext)
                moves.append((path_in, path_out))

    # Check move filenames are unique
    tmp = set()
    for path_in, path_out in moves:
        if path_out in tmp:
            print(f"ERROR: output name {path_out} isn't unique. Consider options -l/-r")
            return 1
        else:
            tmp.add(path_out)

    # Move
    for path_in, path_out in moves:
        if config['dry_run']:
            print(f'Move {path_in} -> {path_out}')
        else:
            os.rename(path_in, path_out)


if __name__ == '__main__':
    sys.exit(main())
