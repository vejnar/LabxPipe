# -*- coding: utf-8 -*-

#
# Copyright (C) 2013-2022 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

"""Interface with the `Bowtie2 <https://github.com/BenLangmead/bowtie2>`_ executable program."""

import os
import re
import subprocess

bowtie2_quality_scores = {'Solexa':'--solexa-quals', 'Illumina 1.3':'--phred64', 'Illumina 1.5':'--phred64', 'Illumina 1.8':'--phred33'}

def get_bowtie2_version(exe=None):
    # Defaults
    if exe is None:
        exe = 'bowtie2'
    # Get version
    p = subprocess.run([exe, '--version'], check=True, stdout=subprocess.PIPE, text=True)
    return re.search(r'bowtie2-align-s version ([\.\w]+)', p.stdout).group(1)

def bowtie2(fq_1, fq_2=None, outfile=None, quality_score=None, bwt_index=None, num_processor=None, compress_sam=None, compress_sam_cmd=None, others=None, exe=None, cwd=None, return_std=None, logger=None):
    # Defaults
    if exe is None:
        exe = 'bowtie2'
    if logger is None:
        import logging as logger
    # ---------
    # Bowtie2 cmd & parameters
    cmd = [exe]
    # Number of threads
    if num_processor is not None:
        cmd.append('--threads')
        cmd.append(str(num_processor))
    # Bowtie2 index
    if bwt_index is None:
        raise ValueError('Bowtie2 index parameter is required.')
    else:
        cmd.append('-x')
        cmd.append(bwt_index)
        # Input reads
    if fq_2 is not None:
        cmd.extend(['-1', ','.join(fq_1)])
        cmd.extend(['-2', ','.join(fq_2)])
    else:
        cmd.extend(['-U', ','.join(fq_1)])
    # Output aln
    if outfile is not None:
        cmd.append('-S')
        cmd.append(outfile)
    # Quality scores
    if quality_score is not None:
        if quality_score in bowtie2_quality_scores:
            cmd.append(bowtie2_quality_scores[quality_score])
        else:
            cmd.append(quality_score)
    # Add remaining parameters
    if others is not None:
        cmd.extend(others)
    # Set default output compression
    if compress_sam and compress_sam_cmd is None:
        compress_sam_cmd = ['zstd', '--rm', '-12', '-T'+num_processor]
    # ---------
    # Start Bowtie2
    logger.info('Starting Bowtie2 with ' + str(cmd))
    if return_std:
        try:
            p = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, cwd=cwd)
        except Exception as e:
            logger.error('Bowtie2 failed: ' + e.stderr)
            raise
    else:
        subprocess.run(cmd, check=True)
    # ---------
    # Post-processing
    outpath = os.path.dirname(outfile)
    if compress_sam:
        for fname in os.listdir(outpath):
            if fname.endswith('.sam'):
                subprocess.run(compress_sam_cmd + [os.path.join(outpath, fname)], check=True)
    if return_std:
        return p.stdout, p.stderr

def get_bowtie2_report(path_output):
    # Read report 
    raw_report = open(path_output, 'rt').read()
    # Parse Bowtie2 output for un-paired sequencing
    result = re.search(r'(?P<input>\d+) reads; of these:\s+(?P<input_unpaired>\d+) \(\S+\) were unpaired; of these:\s+(?P<no_align>\d+) \(\S+\) aligned 0 times\s+(?P<align_unique>\d+) \(\S+\) aligned exactly 1 time\s+(?P<align_multi>\d+)', raw_report)
    if result:
        report = {'no_align': int(result.group('no_align')),
                  'align_unique': int(result.group('align_unique')),
                  'align_multi': int(result.group('align_multi')),
                  'input_unpaired': int(result.group('input_unpaired')),
                  'input': int(result.group('input'))}
        report['output'] = report['align_unique'] + report['align_multi']
        return report
    # Parse Bowtie2 output for paired sequencing
    re_paired = r'(?P<input>\d+) reads; of these:\s+(?P<input_paired>\d+) \(\S+\) were paired; of these:\s+(?P<no_align>\d+) \(\S+\) aligned concordantly 0 times\s+(?P<align_unique>\d+) \(\S+\) aligned concordantly exactly 1 time\s+(?P<align_multi>\d+) \(\S+\) aligned concordantly >1 times'
    # If option --no-discordant wasn't used
    if 'aligned discordantly' in raw_report:
        re_paired += r'\s+----\s+(\d+) pairs aligned concordantly 0 times; of these:\s+(?P<no_align_align_unique_discordant>\d+) \(\S+\) aligned discordantly 1 time'
    # If option --no-mixed wasn't used
    if 'concordantly or discordantly' in raw_report:
        re_paired += r'\s+----\s+(\d+) pairs aligned 0 times concordantly or discordantly; of these:\s+(\d+) mates make up the pairs; of these:\s+(?P<discordant_mate_no_align>\d+) \(\S+\) aligned 0 times\s+(?P<discordant_mate_unique>\d+) \(\S+\) aligned exactly 1 time\s+(?P<discordant_mate_multi>\d+) \(\S+\) aligned >1 times'
    result = re.search(re_paired, raw_report)
    if result:
        report = {'no_align': int(result.group('no_align')),
                  'align_unique': int(result.group('align_unique')),
                  'align_multi': int(result.group('align_multi')),
                  'input_paired': int(result.group('input_paired')),
                  'input': int(result.group('input'))}
        if 'aligned discordantly' in raw_report:
            report['no_align_align_unique_discordant'] = int(result.group('no_align_align_unique_discordant'))
        if 'concordantly or discordantly' in raw_report:
            report['discordant_mate_no_align'] =  int(result.group('discordant_mate_no_align'))
            report['discordant_mate_unique'] =  int(result.group('discordant_mate_unique'))
            report['discordant_mate_multi'] = int(result.group('discordant_mate_multi'))
        report['output'] = report['align_unique'] + report['align_multi']
        return report
