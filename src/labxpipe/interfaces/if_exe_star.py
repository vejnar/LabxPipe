# -*- coding: utf-8 -*-

#
# Copyright (C) 2013-2022 Charles E. Vejnar
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://www.mozilla.org/MPL/2.0/.
#

"""Interface with the `STAR <https://github.com/alexdobin/STAR>`_ executable program."""

import os
import re
import subprocess

star_quality_scores = {'Solexa':'-26', 'Illumina 1.3':'-31', 'Illumina 1.5':'-31', 'Illumina 1.8':None}

def get_star_version(exe=None):
    # Defaults
    if exe is None:
        exe = 'STAR'
    # Get version
    return subprocess.run([exe, '--version'], stdout=subprocess.PIPE, text=True).stdout.strip()

def star(fq_1, fq_2=None, outpath=None, quality_score=None, reads_directional=False, star_index=None, num_processor=None, output_type=None, rename=None, compress_sam=None, compress_sam_cmd=None, compress_unmapped=None, compress_unmapped_cmd=None, others=None, exe=None, return_std=None, logger=None):
    # Defaults
    if exe is None:
        exe = 'STAR'
    if logger is None:
        import logging as logger
    # ---------
    # STAR cmd & parameters
    cmd = [exe]
    # Number of threads
    if num_processor is not None:
        cmd.append('--runThreadN')
        cmd.append(num_processor)
    # Output aln
    if outpath is not None:
        cmd.append('--outFileNamePrefix')
        cmd.append(outpath)
    # Quality scores
    if quality_score is not None:
        if quality_score in star_quality_scores:
            if star_quality_scores[quality_score] is not None:
                cmd.append('--outQSconversionAdd')
                cmd.append(star_quality_scores[quality_score])
        else:
            cmd.append('--outQSconversionAdd')
            cmd.append(quality_score)
    # Library type
    if not reads_directional:
        cmd.append('--outSAMstrandField')
        cmd.append('intronMotif')
    # STAR index
    if star_index is None:
        raise ValueError('STAR index parameter is required.')
    else:
        cmd.append('--genomeDir')
        cmd.append(star_index)
    # Output type
    if output_type is not None:
        cmd.append('--outSAMtype')
        cmd.extend(output_type)
    # Add remaining parameters
    if others is not None:
        cmd.extend(others)
    # Reads
    reads = ['--readFilesIn', ','.join(fq_1)]
    if fq_2 is not None:
        reads.append(','.join(fq_2))
    cmd.extend(reads)
    # Add zcat if necessary
    if fq_1[0].endswith('.gz'):
        cmd.append('--readFilesCommand')
        cmd.append('zcat')
    # Add lz4 if necessary
    if fq_1[0].endswith('.lz4'):
        cmd.append('--readFilesCommand')
        cmd.append('lz4cat')
    # Add zstd if necessary
    if fq_1[0].endswith('.zst'):
        cmd.append('--readFilesCommand')
        cmd.append('zstdcat')
    # Set default output compression
    if compress_sam and compress_sam_cmd is None:
        compress_sam_cmd = ['zstd', '--rm', '-10', '-T'+num_processor]
    if compress_unmapped and compress_unmapped_cmd is None:
        compress_unmapped_cmd = ['zstd', '--rm', '-10', '-T'+num_processor]
    # ---------
    # Start STAR
    logger.info('Starting STAR with ' + str(cmd))
    if return_std:
        try:
            p = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        except Exception as e:
            logger.error('STAR failed: ' + e.stderr)
            raise
    else:
        subprocess.run(cmd, check=True)
    # ---------
    # Post-processing
    if rename:
        for fname, fname_new, fname_alt in [('Aligned.out.sam', 'accepted_hits.sam', 'accepted_hits.sam'), ('Aligned.out.bam', 'accepted_hits.bam', 'accepted_hits.bam'), ('Aligned.sortedByCoord.out.bam', 'accepted_hits.bam', 'accepted_hits_sorted.bam'), ('Unmapped.out.mate1', 'unmapped_R1.fastq', 'unmapped_R1.fastq'), ('Unmapped.out.mate2', 'unmapped_R2.fastq', 'unmapped_R2.fastq')]:
            if os.path.exists(os.path.join(outpath, fname)):
                if os.path.exists(os.path.join(outpath, fname_new)):
                    os.rename(os.path.join(outpath, fname), os.path.join(outpath, fname_alt))
                else:
                    os.rename(os.path.join(outpath, fname), os.path.join(outpath, fname_new))
    if compress_sam:
        for fname in os.listdir(outpath):
            if fname.endswith('.sam'):
                subprocess.run(compress_sam_cmd + [os.path.join(outpath, fname)], check=True)
    if compress_unmapped and '--outReadsUnmapped' in cmd:
        for fname in ['Unmapped.out.mate1', 'unmapped_R1.fastq', 'Unmapped.out.mate2', 'unmapped_R2.fastq']:
            outfname = os.path.join(outpath, fname)
            if os.path.exists(outfname):
                subprocess.run(compress_unmapped_cmd + [outfname], check=True)
    if return_std:
        return p.stdout, p.stderr

def star_index(star_index=None, path_seqs=None, num_processor=None, exe=None, logger=None):
    # Defaults
    if exe is None:
        exe = 'STAR'
    if logger is None:
        import logging as logger
    # ---------
    # STAR cmd & parameters
    cmd = [exe, '--runMode', 'genomeGenerate']
    # STAR index
    if star_index is None:
        raise ValueError('STAR index parameter is required.')
    else:
        if not os.path.exists(star_index):
            os.mkdir(star_index)
        cmd.append('--genomeDir')
        cmd.append('.')
    # Sequence(s)
    if path_seqs is None:
        raise ValueError('STAR sequence parameter is required.')
    else:
        cmd.append('--genomeFastaFiles')
        cmd.extend(path_seqs)
    # Number of threads
    if num_processor is not None:
        cmd.append('--runThreadN')
        cmd.append(num_processor)
    # ---------
    # Start STAR
    logger.info('Starting STAR with ' + str(cmd))
    p = subprocess.run(cmd, text=True, check=True, cwd=star_index)

def parse_star_report(path_output):
    report = {'output': 0}
    raw_report = open(path_output, 'rt').read()
    result = re.search(r'Number of input reads \|\t(\d+)\s', raw_report)
    if result:
        report['input'] = int(result.group(1))
    result = re.search(r'Uniquely mapped reads number \|\t(\d+)\s', raw_report)
    if result:
        report['align_unique'] = int(result.group(1))
        report['output'] += report['align_unique']
    result = re.search(r'Number of reads mapped to multiple loci \|\t(\d+)\s', raw_report)
    if result:
        report['align_multi'] = int(result.group(1))
        report['output'] += report['align_multi']
    return report
