# <img src="https://raw.githubusercontent.com/vejnar/LabxPipe/main/img/logo.svg" alt="LabxPipe" width="45%" />

[![MPLv2](https://img.shields.io/aur/license/python-labxpipe?color=1793d1&style=for-the-badge)](https://mozilla.org/MPL/2.0/)

* Integrated with [LabxDB](https://labxdb.vejnar.org): all required annotations (labels, strand, paired etc) are retrieved from LabxDB. This is optional.
* Based on existing robust technologies. No new language.
    * LabxPipe pipelines are defined in JSON text files.
    * LabxPipe is written in Python. Using norms, such as input and output filenames, insures compatibility between tasks.
* Simple and complex pipelines.
    * By default, pipelines are linear (one step after the other).
    * Branching is easily achieved be defining a previous step (using `step_input` parameter) allowing users to create any dependency between tasks.
* Parallelized using robust asynchronous threads from the Python standard library.

## Commands

LabxPipe provides a unique `lxpipe` command with multiples sub-commands. Running a pipeline would typically involve using these sub-commands:

<img src="https://raw.githubusercontent.com/vejnar/LabxPipe/main/img/commands.svg" alt="LabxPipe" />

The output of multiple pipelines executed using `lxpipe run` can be combined to merge gene counts or create profiles and trackhubs with the following sub-commands:

<img src="https://raw.githubusercontent.com/vejnar/LabxPipe/main/img/multi_commands.svg" alt="LabxPipe" />

See examples to understand how each sub-command works.

## Examples

See JSON files in `config/pipelines` of this repository.

| Pipeline JSON file            |                                                                                                                           |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `mrna_seq.json`               | mRNA-seq.                                                                                                                 |
| `mrna_seq_profiling_bam.json` | mRNA-seq. Genomic coverage profiles using [GeneAbacus](https://sr.ht/~vejnar/GeneAbacus). BAM and SAM outputs.            |
| `mrna_seq_no_db.json`         | mRNA-seq. No [LabxDB](https://labxdb.vejnar.org).                                                                         |
| `mrna_seq_with_plotting.json` | mRNA-seq. Plotting non-mapped reads. Demonstrate `step_input`.                                                            |
| `mrna_seq_cufflinks.json`     | mRNA-seq. Replaces GeneAbacus by Cufflinks.                                                                               |
| `chip_seq.json`               | ChIP-seq. [Bowtie2](https://github.com/BenLangmead/bowtie2) and [Samtools](http://www.htslib.org) to uniquify reads.      |
| `chip_seq_user_function.json` | ChIP-seq. [Bowtie2](https://github.com/BenLangmead/bowtie2) and [Samtools](http://www.htslib.org) to uniquify reads. Genomic coverage profiles using [GeneAbacus](https://sr.ht/~vejnar/GeneAbacus). Peak-calling using [MACS3](https://github.com/macs3-project/MACS) employing a *user-defined step/function*. |

Following demonstrates how to apply `mrna_seq.json` pipeline. It requires:
* [LabxDB](https://labxdb.vejnar.org)
* FASTQ files for sample named `AGR000850` and `AGR000912`
    ```
    /plus/data/seq/by_run/AGR000850
    ├── 23_009_R1.fastq.zst
    └── 23_009_R2.fastq.zst
    /plus/data/seq/by_run/AGR000912
    ├── 65_009_R1.fastq.zst
    └── 65_009_R2.fastq.zst
    ```

Note: `mrna_seq_no_db.json` demonstrates how to use LabxPipe *without* LabxDB: it only requires FASTQ files (in `path_seq_run` directory, see above).

Requirements:
* [LabxDB](https://labxdb.vejnar.org). Alternatively, `mrna_seq_no_db.json` doesn't require LabxDB.
* [ReadKnead](https://sr.ht/~vejnar/ReadKnead) to trim reads.
* [STAR](https://github.com/alexdobin/STAR) and genome index in directory defined `path_star_index`.
* [GeneAbacus](https://sr.ht/~vejnar/GeneAbacus) to count reads and generate genomic profile for tracks.

1. Start pipeline:
    ```bash
    lxpipe run --pipeline mrna_seq.json \
               --worker 2 \
               --processor 16
    ```
    Output is written in `path_output` directory.
2. Create report:
    ```bash
    lxpipe report --pipeline mrna_seq.json
    ```
    Report file `mrna_seq.xlsx` should be created in same directory as `mrna_seq.json`.
3. Extract output file(s) to use them directly, for instance to load them in IGV. For example:
    * To extract BAM files and rename them using the sample label:
        ```bash
        lxpipe extract --pipeline mrna_seq.json \
                       --files aligning,accepted_hits.sam.zst \
                       --label
        ```
    * To extract BigWig profile files and rename them using the sample label and reference in addition to the original filename used as filename suffix:
        ```bash
        lxpipe extract --pipeline mrna_seq.json \
                       --files profiling,genome_plus.bw \
                       --label \
                       --reference \
                       --suffix
        ```
    Use `-d`/`--dry_run` to test the extract command before applying it.
4. Merge gene/mRNA counts generated by [GeneAbacus](https://sr.ht/~vejnar/GeneAbacus) in `counting` directory:
    ```bash
    lxpipe merge-count --pipeline mrna_seq.json \
                       --step counting
    ```
5. Create a trackhub. Requirements:
    * [ChromosomeMappings](https://github.com/dpryan79/ChromosomeMappings) file (to map chromosome names from Ensembl/NCBI to UCSC)
    * Tabulated file (with chromosome name and length)

    Execute in a separate directory:
    ```bash
    lxpipe trackhub --runs AGR000850,AGR000912 \
                    --species_ucsc danRer11 \
                    --path_genome /plus/scratch/sai/annots/danrer_genome_all_ensembl_grcz11_ucsc_chroms_chrom_length.tab \
                    --path_mapping /plus/scratch/sai/annots/ChromosomeMappings/GRCz11_ensembl2UCSC.txt \
                    --input_sam \
                    --bam_names accepted_hits.sam.zst \
                    --make_config \
                    --make_trackhub \
                    --make_bigwig \
                    --processor 16
    ```
    Directory is ready to be shared by a web server for display in the [UCSC genome browser](https://genome.ucsc.edu/cgi-bin/hgHubConnect).

## Configuration

Parameters can be defined [globally](https://labxdb.vejnar.org/doc/install/python/#configuration). See in `config` directory of this repository for examples.

## Writing pipelines

Parameters are defined first globally (see above), then per pipeline, then per replicate/run, and then per step/function. The latest definition takes precedence: `path_seq_run` defined in `/etc/hts/labxpipe.json` is used by default, but if `path_seq_run` is defined in the pipeline file, it will be used instead.

Main parameters

| Parameter           | Type          |
| ------------------- | ------------- |
| name                | string        |
| path_output         | string        |
| path_seq_run        | string        |
| path_local_steps    | string        |
| path_annots         | string        |
| path_bowtie2_index  | string        |
| path_bwa-mem2_index | string        |
| path_minimap2_index | string        |
| path_star_index     | string        |
| fastq_exts          | []strings     |
| adaptors            | {}            |
| logging_level       | string        |
| run_refs            | []strings     |
| replicate_refs      | []strings     |
| ref_info_source     | []strings     |
| ref_infos           | {}            |
| analysis            | [{}, {}, ...] |

Parameters for all steps

| Parameter     | Type    |
| ------------- | ------- |
| step_name     | string  |
| step_function | string  |
| step_desc     | string  |
| force         | boolean |

Step-specific parameters

| Step               | Synonym          | Parameter             | Type          |
| ------------------ | ---------------- | --------------------- | ------------- |
| readknead          | preparing        | options               | []strings     |
|                    |                  | ops_r1                | [{}, {}, ...] |
|                    |                  | ops_r2                | [{}, {}, ...] |
|                    |                  | plot_fastq_in         | boolean       |
|                    |                  | plot_fastq            | boolean       |
|                    |                  | fastq_out             | boolean       |
|                    |                  | zip_fastq_out         | string        |
| bowtie2            | genomic_aligning | options               | []strings     |
|                    |                  | index                 | string        |
|                    |                  | output                | string        |
|                    |                  | output_unfiltered     | string        |
|                    |                  | compress_sam          | boolean       |
|                    |                  | compress_sam_cmd      | string        |
|                    |                  | create_bam◆           | boolean       |
|                    |                  | index_bam◆            | boolean       |
| bwa-mem2           |                  | options               | []strings     |
|                    |                  | index                 | string        |
|                    |                  | output                | string        |
|                    |                  | compress_output       | boolean       |
|                    |                  | compress_output_cmd   | string        |
|                    |                  | create_bam◆           | boolean       |
|                    |                  | index_bam◆            | boolean       |
| minimap2           |                  | options               | []strings     |
|                    |                  | index                 | string        |
|                    |                  | output                | string        |
|                    |                  | compress_output       | boolean       |
|                    |                  | compress_output_cmd   | string        |
|                    |                  | create_bam◆           | boolean       |
|                    |                  | index_bam◆            | boolean       |
| star               | aligning         | options               | []strings     |
|                    |                  | index                 | string        |
|                    |                  | output_type           | []strings     |
|                    |                  | compress_sam          | boolean       |
|                    |                  | compress_sam_cmd      | string        |
|                    |                  | compress_unmapped     | boolean       |
|                    |                  | compress_unmapped_cmd | string        |
| cufflinks          |                  | options               | []strings     |
|                    |                  | inputs                | [{}, {}, ...] |
|                    |                  | features              | [{}, {}, ...] |
| geneabacus         | counting         | options               | []strings     |
|                    |                  | inputs                | [{}, {}, ...] |
|                    |                  | path_annots           | string        |
|                    |                  | features              | [{}, {}, ...] |
| samtools_sort      |                  | options               | []strings     |
|                    |                  | sort_by_name_bam      | boolean       |
| samtools_uniquify  |                  | options               | []strings     |
|                    |                  | sort_by_name_bam      | boolean       |
|                    |                  | index_bam             | boolean       |
| cleaning           |                  | steps                 | [{}, {}, ...] |

◆ indicates exclusive options. For example, either `create_bam` or `index_bam` can be used, but not both.

Sample-specific parameters. Automatically populated if using LabxDB or sourced from `ref_infos`. These parameters can be changed manually in any step (for example setting `paired` to `false` will ignore second reads in that step).

| Parameter      | Type    |
| -------------- | ------- |
| label_short    | string  |
| paired         | boolean |
| directional    | boolean |
| r1_strand      | string  |
| quality_scores | string  |

## User-defined step

In addition to the provided steps/functions, i.e. `bowtie2`, `star` or `geneabacus`, users can defined their own step, usable in the LabxPipe pipelines. LabxPipe will import user-defined steps:
* Written in Python
* One step per file with the `.py` extension located in the directory defined by `path_local_steps`
* Each step defined in individual file requires:
    1. A `functions` variable listing the step name(s)
    2. A function named `run` with the 3 parameters `path_in`, `path_out` and `params`

    For example:
    ```python
    functions = ['macs3']
    def run(path_in, path_out, params):
        ...
    ```

Example of a user-defined function providing peak-calling using [MACS3](https://github.com/macs3-project/MACS) is available in `config/user_steps/macs3.py` in this repository.

Example of a pipeline using the [MACS3](https://github.com/macs3-project/MACS) step is available in `config/pipelines/chip_seq_user_function.json` in this repository.

## Demultiplexing sequencing reads: `lxpipe demultiplex`

* Demultiplex reads based on barcode sequences from the `Second barcode` field in [LabxDB](https://labxdb.vejnar.org)
* Demultiplexing using [ReadKnead](https://sr.ht/~vejnar/ReadKnead). The most important for demultiplexing is the ReadKnead pipeline. Pipelines are identified using the `Adapter 3'` field in LabxDB.

* Example for simple demultiplexing. The first nucleotides at the 5' end of read 1 are used as barcodes (the `Adapter 3'` field is set to `sRNA 1.5` in LabxDB for these samples) with the following pipeline:
    ```json
    {
        "sRNA 1.5": {
            "R1": [
                {
                    "name": "demultiplex",
                    "end": 5,
                    "max_mismatch": 1
                }
            ],
            "R2": null
        }
    }
    ```
    The barcode sequences are added by LabxPipe using the `Second barcode` field in [LabxDB](https://labxdb.vejnar.org).

* Example for iCLIP demultiplexing. In [Vejnar et al.](https://pubmed.ncbi.nlm.nih.gov/31227602), iCLIP is demultiplexed (the `Adapter 3'` field is set to `TruSeq-DMS+A Index` in LabxDB for these samples) using the following pipeline:
    ```json
    {
        "TruSeq-DMS+A Index": {
            "R1": [
                {
                    "name": "clip",
                    "end": 5,
                    "length": 4,
                    "add_clipped": true
                },
                {
                    "name": "trim",
                    "end": 3,
                    "algo": "bktrim",
                    "min_sequence": 5,
                    "keep": ["trim_exact", "trim_align"]
                },
                {
                    "name": "length",
                    "min_length": 6
                },
                {
                    "name": "demultiplex",
                    "end": 3,
                    "max_mismatch": 1,
                    "length_ligand": 2
                },
                {
                    "name": "length",
                    "min_length": 15
                }
            ],
            "R2": null
        }
    }
    ```
    Pipeline is stored in `demux_truseq_dms_a.json`. The barcode sequences are added by LabxPipe using the `Second barcode` field in [LabxDB](https://labxdb.vejnar.org). (NB: published demultiplexed data were generated using `"algo": "align"` with a minimum score of 80 instead of `"algo": "bktrim"`)

    Then pipeline was tested running:
    ```bash
    lxpipe demultiplex --bulk HHYLKADXX \
                       --path_demux_ops demux_truseq_dms_a.json \
                       --path_seq_prepared prepared \
                       --demux_nozip \
                       --processor 1 \
                       --demux_verbose_level 20 \
                       --no_readonly
    ```
    This output is **very verbose**: for every read, output from every step of the demultiplexing pipeline is reported. To get consistent output, `--processor` must be set to `1`. Output is written in local directory `prepared`.

    And finally, once pipeline is validated (data is written in `path_seq_prepared` directory, see [here](https://labxdb.vejnar.org/doc/install/python/#configuration)):
    ```bash
    lxpipe demultiplex --bulk HHYLKADXX \
                       --path_demux_ops demux_truseq_dms_a.json \
                       --processor 10
    ```

## License

*LabxPipe* is distributed under the Mozilla Public License Version 2.0 (see /LICENSE).

Copyright © 2013-2023 Charles E. Vejnar
