#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# Copyright (C) 2013-2022 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

"""Generate and convert project files"""

import argparse
import json
import os
import sys

import labxdb

def main(argv=None):
    if argv is None:
        argv = sys.argv
    # Started from wrapper?
    prog = os.path.basename(argv[0])
    if argv[1] == 'generate':
        job_cmd = argv[:2]
        argv_parser = argv[2:]
        prog += ' generate'
    else:
        job_cmd = argv[:1]
        argv_parser = argv[1:]
    # Parse arguments
    parser = argparse.ArgumentParser(prog=prog, description='Generate pipeline config.')
    parser.add_argument('-p', '--project', dest='project', action='store', help='Project')
    parser.add_argument('-r', '--replicate', dest='replicate', action='store_true', help='List replicates instead of runs')
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

    # Init. DBLink
    if 'labxdb_http_path' not in config and 'labxdb_http_db' not in config:
        if 'labxdb_http_path_seq' in config:
            config['labxdb_http_path'] = config['labxdb_http_path_seq']
        else:
            config['labxdb_http_db'] = 'seq'
    dbl = labxdb.DBLink(config.get('labxdb_http_url'), config.get('labxdb_http_login'), config.get('labxdb_http_password'), config.get('labxdb_http_path'), config.get('labxdb_http_db'))

    # Generate new pipeline from project ID
    if 'project' in config:
        # Empty project
        empty_project = {'name': '', 'path_output': '', 'logging_level': 'info'}
        # Add refs
        projects = dbl.post('tree', {'search_criterion':['0 project_ref EQUAL '+config['project']], 'search_gate':'AND', 'limit':'ALL'})
        if args.replicate:
            refs = sorted([replicate['replicate_ref'] for project in projects for sample in project['children'] for replicate in sample['children']])
            empty_project['replicate_refs'] = refs
        else:
            refs = sorted([run['run_ref'] for project in projects for sample in project['children'] for replicate in sample['children'] for run in replicate['children']])
            empty_project['run_refs'] = refs
        # Add analysis
        empty_project['analysis'] = []
        # Print
        print(json.dumps(empty_project, indent=4, separators=(',', ': ')))

if __name__ == '__main__':
    sys.exit(main())
