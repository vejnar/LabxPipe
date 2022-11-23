#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# Copyright (C) 2013-2022 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

import argparse
import sys

import labxpipe_scripts.lxpipe_demultiplex
import labxpipe_scripts.lxpipe_generate
import labxpipe_scripts.lxpipe_merge_count
import labxpipe_scripts.lxpipe_profile
import labxpipe_scripts.lxpipe_run
import labxpipe_scripts.lxpipe_report
import labxpipe_scripts.lxpipe_trackhub

subcommands = {
    'run': labxpipe_scripts.lxpipe_run,
    'report': labxpipe_scripts.lxpipe_report,
    'merge-count': labxpipe_scripts.lxpipe_merge_count,
    'generate': labxpipe_scripts.lxpipe_generate,
    'profile': labxpipe_scripts.lxpipe_profile,
    'trackhub': labxpipe_scripts.lxpipe_trackhub,
    'demultiplex': labxpipe_scripts.lxpipe_demultiplex
}

def generate_help(subcommands):
    helpmsg = ''
    for k, m in subcommands.items():
        doc = ''
        if m.__doc__ is not None:
            for l in m.__doc__.split('\n'):
                if len(l.strip()) > 0:
                    doc = l.strip()
                    break
        helpmsg += f'{k:<13}{doc}\n'
    return helpmsg

class _HelpAction(argparse._HelpAction):
    def __call__(self, parser, namespace, values, option_string=None):
        if 'command' in namespace and namespace.command in subcommands:
            subcommands[namespace.command].main([f'{parser.prog} {namespace.command}', '-h'])
        else:
            parser.print_help()
            parser.exit()

def main(argv=None):
    if argv is None:
        argv = sys.argv
    parser = argparse.ArgumentParser(description='LabxPipe', formatter_class=argparse.RawTextHelpFormatter, add_help=False)
    parser.add_argument('-h', '--help', action=_HelpAction, help='help for help if you need some help')
    parser.add_argument('command', metavar='command', action='store', choices=subcommands.keys(), help=generate_help(subcommands))
    # One command is required
    if len(argv) < 2:
        parser.print_help()
        return 1
    # Parse arguments
    args, rest = parser.parse_known_args(argv[1:])
    # Run
    subcommands[args.command].main(argv)

if __name__ == '__main__':
    sys.exit(main())
