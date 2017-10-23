# InterPro Protein Update

The InterPro Protein Update is the procedures loading protein data from UniProtKB/Swiss-Prot and UniProtKB/TrEMBL flat files, and updating InterPro production tables to reflect changes.

## Getting started

### Requirements

* Python 3.3+.
* The `numpy`, and `h5py` Python packages.
* the `mundone`, and `pyswiss` Python packages (*included in this repository*).

### Installation

```bash
git clone https://github.com/ProteinsWebTeam/interpro-protein-update.git
cd interpro-protein-update
bash setup.sh
```

## Configuration

Make a copy of `config.ini.sample`, and edit it.

<table>
    <thead>
        <tr>
            <th>Section</th>
            <th>Option</th>
            <th>Description</th>
            <th>Comment</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td rowspan=4>Database</td>
            <td>host</td>
            <td>database TNS</td>
            <td></td>
        </tr>
        <tr>
            <td>user_pro</td>
            <td>interpro user connection string (interpro/********)</td>
            <td></td>
        </tr>
        <tr>
            <td>user_scan</td>
            <td>iprscan user connection string (iprscan/********)</td>
            <td></td>
        </tr>
        <tr>
            <td>user_parc</td>
            <td>uniparc user connection string (uniparc/********)</td>
            <td>Used only for tests, not in production, hence it can be let empty.</td>
        </tr>
        <tr>
            <td rowspan=4>UniProt</td>
            <td>version</td>
            <td>release version (e.g. 2017_07)</td>
            <td></td>
        </tr>
        <tr>
            <td>date</td>
            <td>release date (e.g. 05-Jul-2017)</td>
            <td></td>
        </tr>
        <tr>
            <td>swissprot_file</td>
            <td>UniProtKB/Swiss-Prot flat file path</td>
            <td></td>
        </tr>
        <tr>
            <td>trembl_file</td>
            <td>UniProtKB/TrEMBL flat file path</td>
            <td></td>
        </tr>
        <tr>
            <td rowspan=3>Directories</td>
            <td>out</td>
            <td>output directory</td>
            <td>HDF5 and some log files</td>
        </tr>
        <tr>
            <td>tmp</td>
            <td>temporary directory</td>
            <td></td>
        </tr>
        <tr>
            <td>tab</td>
            <td>table files directory</td>
            <td>For xref_summary table files</td>
        </tr>
        <tr>
            <td>Cluster</td>
            <td>queue</td>
            <td>LSF queue name</td>
            <td></td>
        </tr>
        <tr>
            <td rowspan=5>Mail</td>
            <td>server</td>
            <td>mail server host</td>
            <td></td>
        </tr>
        <tr>
            <td>sender</td>
            <td>sender address</td>
            <td>Your EBI email address, or the team email address</td>
        </tr>
        <tr>
            <td>interpro</td>
            <td>InterPro team email address</td>
            <td></td>
        </tr>
        <tr>
            <td>aa</td>
            <td>Automated Automation team email address</td>
            <td></td>
        </tr>
        <tr>
            <td>uniprot</td>
            <td>UniProt team email address</td>
            <td></td>
        </tr>        
    </tbody>
<table>


## Workflow overview

<table>
    <thead>
        <tr>
            <th>Step</th>
            <th>Task</th>
            <th>Description</th>
            <th>Comment</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td rowspan=4>Update 1A</td>
            <td>load_swissprot</td>
            <td>Stores UniProtKB/Swiss-Prot proteins in an HDF5 file</td>
            <td></td>
        </tr>
        <tr>
            <td>load_trembl</td>
            <td>Stores UniProtKB/TrEMBL proteins in an HDF5 file</td>
            <td></td>
        </tr>
        <tr>
            <td>dump_db</td>
            <td>	Stores proteins in the InterPro database in an HDF5 file</td>
            <td></td>
        </tr>
        <tr>
            <td>merge_h5</td>
            <td>Concatenates Swiss-Prot and TrEMBL proteins</td>
            <td></td>
        </tr>
        <tr>
            <td>insert_proteins</td>
            <td>Inserts protein changes and new proteins</td>
            <td></td>
        </tr>
        <tr>
            <td>method_changes</td>
            <td>Finds changes to assignments of signatures to InterPro entries</td>
            <td></td>
        </tr>
        <tr>
            <td>Update 1B</td>
            <td>update_proteins</td>
            <td>Updates production tables with protein data</td>
            <td></td>
        </tr>
        <tr>
            <td>UniParc.xref</td>
            <td>uniparc_xref</td>
            <td>Updates cross-references from UniParc</td>
            <td></td>
        </tr>
        <tr>
            <td>Pre-check IPRSCAN</td>
            <td>iprscan_precheck</td>
            <td>Checks if MV_IPRSCAN is ready (i.e. UniParc matches update completed)</td>
            <td rowspan=2>Skipped, unless explicitly called</td>
        </tr>
        <tr>
            <td>Refresh IPRSCAN</td>
            <td>iprscan_refresh</td>
            <td>Refreshes MV_IPRSCAN with the latest data from ISPRO</td>
        </tr>
        <tr>
            <td>Check IPRSCAN</td>
            <td>iprscan_check</td>
            <td>Generates the IPRSCAN health check</td>
            <td></td>
        </tr>
        <tr>
            <td>Refresh METHOD2SWISS_DE</td>
            <td>method2swiss</td>
            <td>Populates the METHOD2SWISS_DE table with Swiss-Prot descriptions</td>
            <td>Required by Happy Helper</td>
        </tr>
        <tr>
            <td>Update 2</td>
            <td>prepare_matches</td>
            <td>Finds new matches</td>
            <td>A pre-production report is generated, and must be checked</td>
        </tr>
        <tr>
            <td>Refresh AA_IPRSCAN</td>
            <td>aa_iprscan</td>
            <td>Recreate a materialized view with up-to-date data from MV_IPRSCAN</td>
            <td></td>
        </tr>
        <tr>
            <td>Update 3</td>
            <td>update_matches</td>
            <td>Updates production tables with match data</td>
            <td></td>
        </tr>
        <tr>
            <td>Check CRC64</td>
            <td>crc64</td>
            <td>Deletes mismatched CRC64 in the protein table</td>
            <td></td>
        </tr>
        <tr>
            <td>Report method changes</td>
            <td>report_method_changes</td>
            <td>Final report that includes deleted, moved, and new signatures</td>
            <td></td>
        </tr>
        <tr>
            <td>Update SITE_MATCH</td>
            <td>site_match</td>
            <td>Inserts new matches into the SITE_MATCH table</td>
            <td></td>
        </tr>
        <tr>
            <td>XREF summary</td>
            <td>dump_xref</td>
            <td>Updates the XREF_SUMMARY table and dumps tab files</td>
            <td></td>
        </tr>
    </tbody>
</table>

## Running a step

```bash
python ipucli.py -c CONFIG -t [TASK [TASK ...]]
```

Where `CONFIG` is the path to the configuration file, and `TASK` are task names.
