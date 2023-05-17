#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# Copyright © 2023 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

"""Interface with the `bwa-mem2 <https://github.com/bwa-mem2/bwa-mem2>`_ executable program."""

import os
import re
import subprocess
import threading


def start_compress_thread(cmd, path_fifo):
    def fn_thread(cmd, path_fifo):
        # Open FIFO
        fifo = os.open(path_fifo, os.O_RDWR)
        # Start cmd
        p = subprocess.Popen(cmd, stdout=fifo)
        # Wait to finish then close FIFO
        # Closing FIFO allows bwa-mem2 to end
        p.wait()
        os.close(fifo)
        return

    thread = threading.Thread(target=fn_thread, args=(cmd, path_fifo))
    thread.start()
    return thread


def get_bwa_mem2_version(exe=None):
    # Defaults
    if exe is None:
        exe = 'bwa-mem2'
    # Get version
    p = subprocess.run([exe, 'version'], check=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    return p.stdout.strip()


def bwa_mem2(
    path_inputs=[],
    command='mem',
    outfile=None,
    index=None,
    num_processor=None,
    compress_output=None,
    compress_output_cmd=None,
    others=None,
    exe=None,
    cwd=None,
    return_std=None,
    logger=None,
):
    # Defaults
    if exe is None:
        exe = 'bwa-mem2'
    if logger is None:
        import logging as logger
    # ---------
    # bwa-mem2 cmd & parameters
    cmd = [exe, command]
    # Number of threads
    if num_processor is not None:
        cmd.append('-t')
        cmd.append(str(num_processor))
    # Output
    if outfile is not None:
        cmd.append('-o')
        cmd.append(outfile)
    # Output path
    if cwd is not None:
        path_output = cwd
    elif outfile is not None:
        path_output = os.path.dirname(outfile)
    else:
        path_output = '.'
    # Add remaining parameters
    if others is not None:
        cmd.extend(others)
    # bwa-mem2 index
    if index is None:
        raise ValueError('bwa-mem2 index parameter is required.')
    else:
        cmd.append(index)
    # Set default output compression
    if compress_output and compress_output_cmd is None:
        compress_output_cmd = ['zstd', '--rm', '-12']
        if num_processor is not None:
            compress_output_cmd.append(f'-T{num_processor}')
    # ---------
    # Start bwa-mem2
    fifos = []
    try:
        # Prepare input
        for i, path_input in enumerate(path_inputs):
            # Add zcat if necessary
            if path_input.endswith('.gz'):
                compress_input_cmd = ['zcat']
            # Add lz4 if necessary
            elif path_input.endswith('.lz4'):
                compress_input_cmd = ['lz4cat']
            # Add zstd if necessary
            elif path_input.endswith('.zst'):
                compress_input_cmd = ['zstdcat']
            else:
                compress_input_cmd = None
            if compress_input_cmd:
                # Open FIFO
                path_fifo = os.path.join(path_output, f'input{i}')
                os.mkfifo(path_fifo)
                fifos.append(path_fifo)
                # Start FIFO thread
                compress_thread = start_compress_thread(compress_input_cmd + [path_input], path_fifo)
                # Add FIFO to command
                cmd.append(path_fifo)
            else:
                cmd.append(path_input)
        # Start bwa-mem2
        logger.info('Starting bwa-mem2 with ' + str(cmd))
        if return_std:
            try:
                p = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, cwd=cwd)
            except Exception as e:
                logger.error('bwa-mem2 failed: ' + e.stderr)
                raise
        else:
            subprocess.run(cmd, check=True)
    finally:
        # Delete input FIFOs
        for f in fifos:
            if os.path.exists(f):
                os.remove(f)
    # ---------
    # Post-processing
    if compress_output and os.path.exists(outfile):
        cmdc = compress_output_cmd + [os.path.join(path_output, outfile)]
        logger.info('Compressing out with ' + str(cmdc))
        subprocess.run(cmdc, check=True)
    if return_std:
        return p.stdout, p.stderr


def get_bwa_mem2_report(path_output):
    # Read report
    raw_report = open(path_output, 'rt').read()
    # Parse input
    report = {'input': 0}
    for m in re.finditer(r'Processed (?P<nseq>\d+) reads', raw_report):
        report['input'] += int(m.group('nseq'))
    return report
