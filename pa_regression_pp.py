#!/p/hdk/cad/PowerportalBase/v1.1/miniconda3/bin/python

# Run the following scripts:
# (1) For each PAR in the regression's PAR list, run Bhanu's Indicators script on <POWER_FLY_CENTRAL_AREA_PATH> if the same, otherwise on <Local Path to Central FSDB Model>:
#     /nfs/site/disks/core.trace.repo/bin/power_artist_pp/dev/RWC_LNC_Comp_PA_LatchFlop.py -r <POWER_FLY_CENTRAL_AREA_PATH>/power_artist_results --centralarea -d <--report_dir> -p lnc --indicator --old --cdyndata -c <--report_dir>/<PAR>_CdynDataInFile.csv -u <PAR>
#     By default, the Indicators shall be placed in the --fsdb_src_area so they can be copied to --fsdb_dst_area for archiving
# (2) Run Bhanu's Indicator roll up script once all the scripts in (4) finish:
#     /usr/bin/head -n1 <--report_dir>/par_exe_power_artist_results_Summary.csv > <--report_dir>/$MODEL_ROOT_$DIE_NAME_Summary.csv
#     /usr/bin/egrep -v "Test,Hier" <--report_dir>/*_power_artist_results_Summary.csv | /usr/bin/sed -e 's/.*_Summary.csv://' >> <--report_dir>/$MODEL_ROOT_$DIE_NAME_Summary.csv
#     /nfs/site/disks/core.trace.repo/bin/power_artist_pp/dev/LNC_PA_IndicatorRollup.py -r <--report_dir>
# (3) Send a mail containing the CSV files (1 for Core and one for each of the PARs) to <indicator_mail_list>
#     Example of report to be mailed.  CUrrently the Cdyndata_out.csv file is not used.
#     All these files can be ziped together into one file prior to mailing:
# ls -lh /nfs/site/disks/suchimod_wa/bvempati/PA_Reports_LNC/core-lnc-a0-22ww30a_clone/client_m_n3_master/temp
# total 32M
# drwxr-s--- 2 bvempati soc 4.0K Jul 21 07:47 ./
# drwxr-s--- 3 bvempati soc 8.0K Aug  8 15:12 ../
# -rw-r----- 1 bvempati soc 6.3M Jul 21 07:47 AllPars_IndicatorRollup.csv
# -rw-r----- 1 bvempati soc  13M Jul 21 07:45 Cdyndata_out.csv
# -rw-r----- 1 bvempati soc  99K Jul 21 07:47 PA_IndicatorRollup.csv
# -rw-r----- 1 bvempati soc 5.8M Jul 21 07:45 core-lnc-a0-22ww30a_clone_clientM_Summary.csv
# -rw-r----- 1 bvempati soc 242K Jul 21 07:47 par_exe_PA_IndicatorRollup.csv
# -rw-r----- 1 bvempati soc 249K Jul 21 07:47 par_exe_int_PA_IndicatorRollup.csv
# -rw-r----- 1 bvempati soc 1.3M Jul 21 07:47 par_fe_PA_IndicatorRollup.csv
# -rw-r----- 1 bvempati soc 220K Jul 21 07:47 par_fma_PA_IndicatorRollup.csv
# -rw-r----- 1 bvempati soc 686K Jul 21 07:47 par_meu_PA_IndicatorRollup.csv
# -rw-r----- 1 bvempati soc 476K Jul 21 07:47 par_mlc_PA_IndicatorRollup.csv
# -rw-r----- 1 bvempati soc 913K Jul 21 07:47 par_msid_PA_IndicatorRollup.csv
# -rw-r----- 1 bvempati soc 559K Jul 21 07:47 par_ooo_int_PA_IndicatorRollup.csv
# -rw-r----- 1 bvempati soc 562K Jul 21 07:47 par_ooo_vec_PA_IndicatorRollup.csv
# -rw-r----- 1 bvempati soc 885K Jul 21 07:47 par_pm_PA_IndicatorRollup.csv
# -rw-r----- 1 bvempati soc 303K Jul 21 07:47 par_pmhglb_PA_IndicatorRollup.csv
# (4)
# /nfs/site/disks/core.trace.repo/bin/power_artist_pp/dev/fsdb_mtl_to_central_area.lnc.splunk.sh <Local Path to Central FSDB Model> <FSDB Model Name> <POWER_FLY_CENTRAL_AREA_PATH>
# Where:
#   <Local Path to Central FSDB Model> = Anirudh's PA workarea (/p/lnc/lnc.pwr.reg/USERS/akoushik/Central_FSDB_Area)
#   <FSDB Model Name> = $MODEL_ROOT
#   <POWER_FLY_CENTRAL_AREA_PATH> = $POWER_FSDB (/p/lnc/power_central_dir/power/system_data/fsdb)
# (5) Compare <Local Path to Central FSDB Model> to <POWER_FLY_CENTRAL_AREA_PATH> to make sure they are the same
#    (other than the control.mtl files)
# (6) If the same, delete <Local Path to Central FSDB Model>, if not, send a mail to <error_mail_list>

import argparse
import datetime
import glob
import io
import logging
import os
import pwd
import smtplib
import socket
import subprocess
import sys
import time
import traceback

from contextlib import contextmanager
from email.message import EmailMessage
from filecmp import dircmp
from multiprocessing import Pool
from pathlib import Path
from typing import List
import CustomLogger
from utils import get_partitions
from ww_utils import get_minus1ww_path, get_rev_tag, get_ww_tag, get_first_previous_ww
from top_ten_dcge import report_top_ten
from upload_to_powerportal import upload_to_powerportal


# Initialize constants used in this script
class CONST(object):
    __slots__                = ()  # Eliminate the ability to change the object dictionary after instantiation
    KB2GB                    = 1048576  # (1,024)^2
    KB2MB                    = 1024

    STR_ENCODING             = 'utf-8'

    PA_WORKAREA              = '/p/lnc/lnc.pwr.reg/USERS/akoushik/Central_FSDB_Area'
    PERF_HOME_PYTHON         = '/p/dpg/arch/perfhome/python/miniconda3/bin/python'
    PA_RESULTS_DIR           = 'power_artist_results'
    SCRIPTS_DIR              = os.path.dirname(os.path.realpath(__file__))
    PWR_DATA_CHECKIN_SCRIPT  = os.path.join(SCRIPTS_DIR, 'fsdb_mtl_to_central_area.lnc.splunk.sh')
    PWR_INDICATORS_SCRIPT    = os.path.join(SCRIPTS_DIR, 'RWC_LNC_Comp_PA_LatchFlop.py')
    PWR_ROLLUP_SCRIPT        = os.path.join(SCRIPTS_DIR, 'LNC_PA_IndicatorRollup.py')
    PA_HIER_SUMMARY          = os.path.join(SCRIPTS_DIR, 'gen_power_artist_hierarchical_summary.py')
    MOSAIC_LATEST            = '/p/dpg/arch/perfhome/mosaic/latest/'
    MOSAIC_DEPENDENCY        = '/p/dpg/arch/perfhome/mosaic/packages/'
    MOSAIC_COMPARE           = f'{PERF_HOME_PYTHON} -m mosaic.cli compare'
    FSDB_SRC_DST_CMP_IGNORES = ['control.mtl',]

CONST = CONST()


class CmdLineError(Exception):
    '''Indicated that command line argument has an invalid value or is missing'''
    def __init__(self, msg: str) -> None:
        self.msg = msg

    def __str__(self) -> str:
        return self.msg

class DataMissingError(Exception):
    '''Indicated that command line argument has an invalid value or is missing'''
    def __init__(self, msg: str) -> None:
        self.msg = msg

    def __str__(self) -> str:
        return self.msg

class RunError(Exception):
    '''Indicated that there was a problem running the generated command line'''
    def __init__(self, msg: str) -> None:
        self.msg = msg

    def __str__(self) -> str:
        return self.msg


# These routines implement a primitive stopwatch
# with start, stop, and lap functionality.  The
# stopwatch can be used to measure wall-clock
# times for routines


class Stopwatch:
    def __init__(self, logger_ptr: logging.Logger):
        self.start_time = datetime.datetime.now().replace(microsecond=0)
        self.stop_time  = datetime.datetime.now().replace(microsecond=0)
        self.logger = logger_ptr

    def start(self, message: str):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.start_time = datetime.datetime.now().replace(microsecond=0)
            self.logger.debug(f'Start: {message}: start_time = {self.start_time}')

    def stop(self, message: str):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.stop_time = datetime.datetime.now().replace(microsecond=0)
            self.logger.debug(f'Stop:  {message}:  stop_time = {self.stop_time}')
            self.logger.debug(f'Delta time = {(self.stop_time - self.start_time)}\n')

    def lap(self, message: str):
        if self.logger.isEnabledFor(logging.DEBUG):
            self.stop_time = datetime.datetime.now().replace(microsecond=0)
            self.logger.debug(f'Lap:   {message}:   lap_time = {self.stop_time}')
            self.logger.debug(f'Delta time = {(self.stop_time - self.start_time)}\n')
            self.start_time = datetime.datetime.now().replace(microsecond=0)


def rm_tree(directory: Path) -> None:
    ''' Recursively remove a directory tree for Python versions < 3.6 where this is not built into Pathlib'''
    for child in directory.iterdir():
        if child.is_file() or child.is_symlink():
            child.unlink()
        else:
            rm_tree(child)
    directory.rmdir()


def exit_error(logger: logging.Logger) -> None:
    '''Make sure logging handlers are flushed & closed and then exit with the specified error'''
    CustomLogger.close_handlers(logger)
    sys.exit(1)


def runCmd(env_vars: dict, cmd: str, logger: logging.Logger, log_success: bool = False) -> str:
    if env_vars is None:
        env_vars = os.environ.copy()

    stopwatch = Stopwatch(logger)
    try:
        stopwatch.start(f'Running command:\n\t{cmd}\n')
        return_code = subprocess.run(cmd, shell=True, capture_output=True, check=True, env=env_vars)
    except subprocess.CalledProcessError as proc_error:
        stopwatch.stop(f'{proc_error.cmd} failed')
        msg = (
            f'Command:\n\t{proc_error.cmd}\nreturned error:\n\t{proc_error.returncode}'
            f'\n\tstdout:\t{" ".join(proc_error.stdout.decode(CONST.STR_ENCODING))}'
            f'\n\tstderr:\t{proc_error.stderr.decode(CONST.STR_ENCODING)}'
        )
        logger.error(msg)
        raise RunError(msg)
    else:
        msg = f'Command {cmd} returned successfully'
        stopwatch.stop(msg)
        if log_success:
            success_msg = (
                f'stdout:\t:{return_code.stdout.decode(CONST.STR_ENCODING)}\n'
                f'stderr:\t:{return_code.stderr.decode(CONST.STR_ENCODING)}'
            )
            logger.debug(f'Output from {cmd} was:\n{success_msg}')

    return msg


def runInPools(env_vars: dict, cmds: List, logger: logging.Logger, log_success: bool = False) -> List:
    logger.debug(f'Running {len(cmds)} commands in parallel')

    results = []
    with Pool(processes=len(cmds)) as pool:
        for cmd in cmds:
            logger.debug(f'Calling run_cmd({cmd} , logger')
            result = pool.apply_async(runCmd, [env_vars, cmd, logger, log_success])
            results.append([cmd, result])

        logger.debug(f'Created {len(cmds)} pools')
        pool.close()
        logger.debug(f'Waiting for {len(cmds)} pools to finish')
        pool.join()
        logger.debug(f'{len(cmds)} pools finished')

    return results

def add_mosaic_args(parser):
    """Add mosaic related arguments"""
    parser.add_argument('-mf',      '--mapping_file',    type=str, required=True, help='Mapping file containing RTL vs COHO domain mappings/formulas')
    parser.add_argument('-mm',      '--meta_cols',       type=str, nargs='*', default='category', help='Column name or list of columns in mapping file that are to be treated as meta information columns')
    parser.add_argument('-mc',      '--metric_col',      type=str, default='metric', help='Column name in mapping file representing metric/counter')
    parser.add_argument('-rdc',     '--rtl_domain_col',  type=str, default='pmon', help='Column name in mapping file containing RTL domain specific event/metric/counter mapping/formulas')
    parser.add_argument('-cdc',     '--coho_domain_col', type=str, default='coho', help='Column name in mapping file containing COHO domain specific event/metric/counter mapping/formulas')
    parser.add_argument('-pi',      '--pass_indicators', type=str, nargs='*', default='end0.cycles', help='List of coho stats that can be used as calisto pass indicator')
    return parser


def parse_args(prog, argv: List) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog=prog, description='Check in Power Artist FSBDs & results and generate and mail Power Indicators')
    parser.add_argument('-b',     '--branch',               type=str, help='Name of the git Cluster branch we are running on')
    parser.add_argument('-cr',    '--core_root',            type=str, help='Location of model being run, defaults to $MODEL_ROOT')
    parser.add_argument('-die',   '--die_name',             type=str, help='DIE to run power analysis on (Default: The TREX $DIE_NAME variable)')
    parser.add_argument('-prod',  '--product_name',         type=str, help='Product name to run on.  If set, will override --die_name the argument')
    parser.add_argument('-s',     '--stepping',             type=str, help='Core step to run the power analysis on')
    parser.add_argument('-r',     '--run_on_pars',          type=str, nargs='*', help='Which partitions to run Power Artist analysis on (Default: All partitions in the project')
    parser.add_argument('-fs',    '--fsdb_src_area',        type=str, required=True, help='Location to take the Power Artist FSDBs and Results from')
    parser.add_argument('-fd',    '--fsdb_dst_area',        type=str, help='Location to place the Power Artist FSDBs and Results, defaults to $POWER_FSDB')
    parser.add_argument('-nc',    '--no_checkin',           action='store_true', help='Do not copy results from --fsdb_src_area to --fsdb_dst_area')
    parser.add_argument('-nd',    '--no_delete',            action='store_true', help='Do not delete --fsdb_src_area even if the same as --fsdb_dst_area')
    parser.add_argument('-rd',    '--report_dir',           type=str, help=f'Directory to place Indicator results.  Default is --fsdb_src_area/power_artist_indicators')
    parser.add_argument('-mle',   '--error_mail_list',      type=str, nargs='*', required=True, help='Mail list to send to if there are errors')
    parser.add_argument('-mli',   '--indicators_mail_list', type=str, nargs='*', required=True, help='Mail list to send indicators results to')
    parser.add_argument('-l',     '--logfile',              type=str, default=f'{os.path.basename(sys.argv[0])}.log', help=f'Log file to use (Default: {os.path.basename(sys.argv[0])}.log')
    parser.add_argument('-v',     '--verbosity',            type=str, choices=['notset', 'debug', 'info', 'warning', 'error', 'critical'], default='warning', help='Log verbosity level (default: warning)')
    parser.add_argument('-q',     '--quiet',                action='store_true', help='Do not print output to the console')
    parser.add_argument('-rp',    '--results_path',         type=str,
                        help=f'Directory which contains the regression results.  Default: $RESULTS_PATH, if set otherwise the directory this script is called from')

    parser = add_mosaic_args(parser)

    return parser.parse_args(argv)


def gen_cmd_line(core_root: Path, die_name: str, branch: str, run_on_pars: List, fsdb_dst_area: Path, report_dir: Path,
                 logger: logging.Logger) -> str:
    msg = ''
    for argv_arg in sys.argv:
        msg += f'{argv_arg} '
    if '--core_root ' not in msg:
        msg += f'--core_root {core_root.resolve()} '
    if '--die_name ' not in msg:
        if die_name:
            msg += f'--die_name {die_name} '
    if '--branch ' not in msg:
        msg += f'--branch {branch} '
    if '--run_on_pars ' not in msg:
        msg += '--run_on_pars '
        for par in run_on_pars:
            msg += f'{par} '
    if '--fsdb_dst_area ' not in msg:
        msg += f'--fsdb_dst_area {fsdb_dst_area.resolve()} '
    if '--report_dir ' not in msg:
        msg += f'--report_dir {report_dir.resolve()} '
    msg = msg.strip()
    return msg


def mail_users(mail_to_list: list, mail_subject: str, mail_body: str, zipped_files: Path, logger: logging.Logger) -> bool:
    mail_users_rc = True

    email_msg = EmailMessage()
    email_msg['From']    = 'bhanu.prakash.vempati@intel.com'
    email_msg['To']      = ', '.join(mail_to_list)
    email_msg['Subject'] = mail_subject
    email_msg.preamble   = 'Power Artist Indicators Script Results (Need MIME-aware email reader to view)'
    email_msg.set_content(mail_body)  # This is the body text of the mail message

    try:
        # If there is a ZIP file to send, add it as an attachment
        if zipped_files is not None:
            with zipped_files.open('rb') as zip_file:
                email_msg.add_attachment(zip_file.read(),
                                         maintype='application',
                                         subtype='octet-stream',
                                         filename=zipped_files.name)

        # And finally, send the message
        with smtplib.SMTP('localhost') as s:
            s.send_message(email_msg)  # Returns a Dict with failed senders, otherwise an empty dict
                                       # Will only throw an Exception if all receivers cannot be reached
    except Exception as e:
        logger.error(f'Could not send E-mail message with subject "{mail_subject}":\n\t{e}')
        mail_users_rc = False

    return mail_users_rc


def create_checkin_cmd(fsdb_src_area: Path, core_root: Path, fsdb_dst_area: Path, par_name_map: Path, cores: str, logger: logging.Logger) -> str:
    # Check in command example:
    # /nfs/site/disks/core.trace.repo/bin/power_artist_pp/dev/fsdb_mtl_to_central_area.lnc.splunk.sh <Local Path to Central FSDB Model> <FSDB Model Name> <POWER_FLY_CENTRAL_AREA_PATH>

    cmd = (
        f'{CONST.PWR_DATA_CHECKIN_SCRIPT} '
        f'{fsdb_src_area.resolve()} '
        f'{core_root.stem} '
        f'{str(fsdb_dst_area)} '
        f'{str(par_name_map)} '
        f'{cores}'
    )

    logger.debug(f'Built checkin command:\n\t{cmd}')

    return cmd


def compare_fsdb_src_area_to_fsdb_dst_area_and_del(fsdb_src_area: Path, fsdb_dst_area: Path, error_mail_list: list,
                                                   mail_header: str, mail_footer: str, no_delete: bool,
                                                   core_root: Path, branch: str, product_name: str, logger: logging.Logger) -> bool:
    '''Compares src tree to dest and if the same other than control.mtl in the dst, delete src (unless no_delete=True) and return True; else return False'''

    # FIXME: The CONST.PWR_DATA_CHECKIN_SCRIPT script takes fsdb_dst_area and adds automatically
    #   core_root.stem/product_name_branch
    # to the path
    # For this compare to work, we need to do the same
    #
    # Long term the fix is to correct CONST.PWR_DATA_CHECKIN_SCRIPT not to add paths, but rather use whatever
    # is provided to it.  Then fsdb_dst_area can point to the final FSDB archive area amd this method does not
    # need to do any magic
    same_same = True  # Assume command will succeed
    no_del = no_delete
    logger.debug(f'no_del = {no_del}')
    final_fsdb_dst_area = fsdb_dst_area / 'system_data' / 'fsdb' / core_root.stem / f'{product_name}'
    logger.debug(f'Comparing {fsdb_src_area.resolve()} to {str(final_fsdb_dst_area)}')
    dcmp = dircmp(fsdb_src_area, final_fsdb_dst_area, ignore=CONST.FSDB_SRC_DST_CMP_IGNORES)

    if len(dcmp.left_only) != 0:
        file_list = ''
        for file_name in dcmp.left_only:
            file_list += f'\t{file_name}\n'
        mail_subject = f'File mismatch when copying PA results for {core_root.stem} -- {product_name}'
        mail_body = (
            f'{mail_header}'
            f'The following files were not successfully copied from {fsdb_src_area.resolve()} to {str(final_fsdb_dst_area)}:\n'
            f'{file_list}\n'
            f'\tNot deleting: {fsdb_src_area.resolve()}\n'
            f'{mail_footer}'
        )
        logger.log(level=CustomLogger.ALWAYS_PRINT_LVL, msg=mail_body)
        mail_users(error_mail_list, mail_subject, mail_body, None, logger)

        no_del    = True
        same_same = False
    else:
        logger.debug(f'{CustomLogger.GREEN}{fsdb_src_area.resolve()} & {str(final_fsdb_dst_area)} are the same (except for {CONST.FSDB_SRC_DST_CMP_IGNORES}){CustomLogger.END}')

    logger.debug(f'no_del = {no_del}')
    if not no_del:
        logger.info(f'Deleting {fsdb_src_area.resolve()}')
        logger.debug(f'{CustomLogger.RED}FIXME: Even though we requested to delete the src area, we are not deleting it.  Remove the comment preventing delete once we are confident that we can safely delete{CustomLogger.END}')
        #rm_tree(fsdb_src_area)
    else:
        logger.debug(f'Not deleting {fsdb_src_area.resolve()}')

    logger.debug(f'Returning: {same_same}')
    return same_same


def create_indicators_cmds(run_on_pars: List, fsdb_area: Path, report_dir:Path, logger: logging.Logger) -> List:
    '''CONST.PWR_INDICATORS_SCRIPT -r <fsdb_area>/power_artist_results --centralarea -d <report_dir> -p lnc --indicator --old --cdyndata -c <report_dir>/<PAR>_CdynDataInFile.csv -u <PAR>'''
    cmds = list()
    for par in run_on_pars:
        cdyn_file_name = Path(report_dir / f'{par}_CdynDataInFile.csv')
        cmd = (
            f'{CONST.PWR_INDICATORS_SCRIPT}'
            f' -r {fsdb_area.resolve()}/{CONST.PA_RESULTS_DIR}'
            f' --centralarea'
            f' -d {report_dir.resolve()}'
            f' -p lnc'
            f' --indicator'
            f' --old'
            f' --cdyndata'
            f' -c {cdyn_file_name.resolve()}'
            f' -u {par}'
        )
        logger.debug(f'Created command:\n\t{cmd}')
        cmds.append(cmd)

    logger.debug(f'Created {len(cmds)} indicator commands')
    return cmds


def create_mosaic_compare_cmd(regression_area: Path, report_dir: Path, product_name: str, branch: str,
                              mapping_file: Path, meta_cols, metric_col, rtl_domain_col, coho_domain_col,
                              pass_indicators,
                              logger: logging.Logger) -> str :
    '''CONST.MOSAIC_COMPARE '''
    regression_path = str(regression_area.resolve())
    rev = get_rev_tag(regression_path)
    ww_tag = get_ww_tag(regression_path)
    t_minus_1_regression_path = get_minus1ww_path(regression_path)
    study_name = '-'.join(regression_path.split('/')[-2:])
    if isinstance(meta_cols, list):
        meta_cols = ','.join(meta_cols)
    if isinstance(pass_indicators, list):
        pass_indicators = ','.join(pass_indicators)

    cmd = (
        f'PYTHONPATH={CONST.MOSAIC_DEPENDENCY}:{CONST.MOSAIC_LATEST}'
        f' {CONST.MOSAIC_COMPARE}' 
        f' --study_name {study_name}'
        f' --regression-path {regression_area.resolve()}'
        f' --mapping-file-path {mapping_file}'
        f' --mapping-meta-cols {meta_cols}'
        f' --mapping-metric-col {metric_col}'
        f' --output-path {report_dir.resolve()}'        
        f' --rtl-domain-col {rtl_domain_col}'
        f' --coho-domain-col {coho_domain_col}'
        f' --coho-metric-pass-indicators {pass_indicators}'
        f' --include-warmup'
        f' --start-instruction-count 0'
    )

    if t_minus_1_regression_path:
        tminus1_regress_glob_path = os.path.join(t_minus_1_regression_path, 'mosaic_report', '**', 'report.json')
        mosaic_report_paths = glob.glob(tminus1_regress_glob_path)
        if mosaic_report_paths:
            cmd += (
                f' --compare-regress-studies {os.path.dirname(mosaic_report_paths[0])}'
            )
        else:
            logger.debug(f'Unable to find previous regression study in path {t_minus_1_regression_path}')
    else:
        logger.debug(f'Unable to find previous regression path')

    logger.debug(f'Created mosaic compare cmd: {cmd}')
    return cmd

def create_hierarchical_summary_cmds(run_on_pars: List, fsdb_area: Path, report_dir:Path, logger: logging.Logger) -> List:
    '''CONST.PA_HIER_SUMMARY --power_artist_results <fsdb_area>/power_artist_results --partition <PAR> --out <report_dir>/<PAR>_pa_hier_summary.csv'''
    cmds = list()
    for par in run_on_pars:
        pa_hier_summary_file = os.path.realpath(os.path.join(report_dir, f'{par}_pa_hier_summary.csv'))
        cmd = (
            f'{CONST.PA_HIER_SUMMARY}'
            f' --power_artist_results {fsdb_area.resolve()}/{CONST.PA_RESULTS_DIR}'
            f' --out {pa_hier_summary_file}'
            f' --partition {par}'
        )
        logger.debug(f'Created command:\n\t{cmd}')
        cmds.append(cmd)

    logger.debug(f'Created {len(cmds)} power artist hierarchical summary commands')
    return cmds


def roll_up_indicators(env_vars: dict, report_dir: Path, core_root: Path, product_name: str, logger: logging.Logger) -> Path:
    #     /usr/bin/head -n1 <--report_dir>/par_exe_power_artist_results_Summary.csv > <--report_dir>/$MODEL_ROOT_$DIE_NAME_Summary.csv
    #     /usr/bin/egrep -v "Test,Hier" <--report_dir>/*_power_artist_results_Summary.csv | /usr/bin/sed -e 's/.*_Summary.csv://' >> <--report_dir>/$MODEL_ROOT_$DIE_NAME_Summary.csv
    #     CONST.PWR_ROLLUP_SCRIPT -r <--report_dir>

    sync_n_sleep = '/usr/bin/sync; /usr/bin/sync; /usr/bin/sync; /use/bin/sleep 2; /usr/bin/sync; /usr/bin/sync; /usr/bin/sync'

    cmd = f'/usr/bin/head -n1 {report_dir.resolve()}/par_exe_power_artist_results_Summary.csv > {report_dir.resolve()}/{core_root.stem}_{product_name}_Summary.csv'
    runCmd(env_vars, cmd, logger)
    runCmd(env_vars, sync_n_sleep, logger)

    cmd = f'/usr/bin/egrep -v "Test,Hier" {report_dir.resolve()}/*_power_artist_results_Summary.csv | /usr/bin/sed -e \'s/.*_Summary.csv://\' >> {report_dir.resolve()}/{core_root.stem}_{product_name}_Summary.csv'
    runCmd(env_vars, cmd, logger)
    runCmd(env_vars, sync_n_sleep, logger)

    cmd = f'/usr/bin/rm {report_dir.resolve()}/*_power_artist_results_Summary.csv'
    runCmd(env_vars, cmd, logger)
    runCmd(env_vars, sync_n_sleep, logger)

    cmd = f'{CONST.PWR_ROLLUP_SCRIPT} -r {report_dir.resolve()}'
    try:
        runCmd(env_vars, cmd, logger)
    except RunError as e:
        logger.error(f'Failed to run {CONST.PWR_ROLLUP_SCRIPT}. Continuing...')
    runCmd(env_vars, sync_n_sleep, logger)

    zipped_files = Path(report_dir / f'pa_indicators_{core_root.stem}.zip')
    cmd = f'/usr/intel/bin/zip -9vlj {zipped_files.with_suffix("")} {report_dir.resolve()}/*.csv'
    runCmd(env_vars, cmd, logger)
    runCmd(env_vars, sync_n_sleep, logger)

    return zipped_files


def mail_indicators(core_root: Path, mail_footer: str, mail_to_list: List, product_name: str,
                    report_area: Path, keiko2rtl_file: Path, top_ten_filename: str, top_ten: List, zipped_files: Path, powerportal_url: str, logger: logging.Logger):
    mail_subject = f'Power Indicators for {core_root.stem} -- {product_name}'
    # The mail body text is preceded by 3 spaces to work around an Outlook "feature" which removes CRs in Plain Text
    mail_body = (
        f'   Attached, please find Power Artist Indicator roll ups for {core_root.stem} -- {product_name}.\n'
        f'   To view all the Indicators, please unzip the attachment.\n'
        f'\n'
        f'   These results as well as Cluster breakdowns are archived in:\n'
        f'   \t{str(report_area)}\n'
    )

    if powerportal_url is not None:
        mail_body += (
            f'\n'
            f'The PowerPortal link for this release can be found here:\n'
            f'\t{powerportal_url}\n'
        )

    if keiko2rtl_file.exists():
        mail_body += (
            f'\n'
            f'\n'
            f'Keiko vs RTL IPC comparison:\n'
            f'\n'
        )
        with keiko2rtl_file.open() as fin:
            for line in fin:
                mail_body += f'    {line}'
        mail_body += '\n'
    for top_ten_msg in top_ten:
        mail_body += f'\n{top_ten_msg}\n'
    mail_body += f'{mail_footer}'

    mail_users(mail_to_list, mail_subject, mail_body, zipped_files, logger)


def main() -> int:
    return_code = 0

    # Determine start time and save so total run time can be calculated later
    current_time = time.localtime()
    run_start_time = datetime.datetime.now().replace(microsecond=0)
    startTimeStr = time.strftime('%a %d-%b-%Y %H:%M:%S', current_time)

    program_name = os.path.basename(sys.argv[0])
    argv = parse_args(program_name, sys.argv[1:])

    console = None
    if not argv.quiet:
        console = sys.stdout

    logfile = argv.logfile if Path(argv.logfile).suffix == '.log' else f'{argv.logfile}.log'
    logger = CustomLogger.create_logger(program_name, logfile, argv.verbosity.upper(), console)

    try:
        core_root = argv.core_root if argv.core_root else os.environ.get('MODEL_ROOT')
        if not core_root:
            msg = '--core_root not specified on command line and could not extract via $MODEL_ROOT, exiting...'
            raise CmdLineError(msg)
        else:
            core_root = Path(core_root)

        die_name  = argv.die_name if argv.die_name else os.environ.get('DIE_NAME')
        if die_name is None:
            raise CmdLineError(f'--die_name was not supplied on the command line and $DIE_NAME is not set')

        product_name = argv.product_name

        stepping = argv.stepping if argv.stepping else os.environ.get('STEPPING')

        branch   = argv.branch if argv.branch else os.environ.get('branch_is')
        if branch is None:
            raise CmdLineError(f'--branch was not supplied on the command line and $branch_is is not set')

        run_on_pars = get_partitions(core_root, product_name, logger)
        if argv.run_on_pars:
            run_on_pars = argv.run_on_pars if 'all' not in argv.run_on_pars else run_on_pars

        fsdb_src_area = Path(argv.fsdb_src_area)
        if not fsdb_src_area.exists():
            msg = f'--fsdb_src_area {str(fsdb_src_area)} does not exist.  Exiting...'
            raise CmdLineError(msg)

        fsdb_dst_area = argv.fsdb_dst_area if argv.fsdb_dst_area else os.environ.get('POWER_FSDB')
        if fsdb_dst_area is None:
            msg = f'--fsdb_dst_area not supplied on the command line and could not be extracted from $POWER_FSDB.  Exiting...'
            raise CmdLineError(msg)
        # Assert: fsdb_dst_area is not None
        fsdb_dst_area = Path(fsdb_dst_area)
        if not fsdb_dst_area.exists():
            msg = f'--fsdb_dst_area {str(fsdb_dst_area)} does not exist.  Exiting...'
            CmdLineError(msg)

        no_checkin = argv.no_checkin

        no_delete = argv.no_delete

        report_dir = Path(argv.report_dir) if argv.report_dir else Path(fsdb_src_area/f'power_artist_indicators')

        results_path = Path(argv.results_path) if argv.results_path else Path(os.getcwd())
        if os.environ.get('RESULTS_PATH') is not None:
            results_path = Path(f'{os.environ.get("RESULTS_PATH")}')

        error_mail_list = argv.error_mail_list
        indicators_mail_list = argv.indicators_mail_list

        # TODO: Replace this logic with logic (or a command line parameter) which can figure out
        # if we ran on icore0, icore1, or both
        cores = 'core' if 'lnc' in stepping or 'cgc' in stepping else 'icore0'

        cmd_line_txt = gen_cmd_line(core_root, die_name, branch, run_on_pars, fsdb_dst_area, report_dir, logger)

        logger.log(level=CustomLogger.ALWAYS_PRINT_LVL, msg=f'Starting:   {program_name}')
        logger.log(level=CustomLogger.ALWAYS_PRINT_LVL, msg='Called as:')
        logger.log(level=CustomLogger.ALWAYS_PRINT_LVL, msg=cmd_line_txt)

        if argv.run_on_pars is not None and 'all' in argv.run_on_pars:
            msg = (
                f'--run_on_pars all called on the command line.\n'
                f'To guarantee you are re-running with the exact same partition list, you can call it explicitly:\n'
                f'\t--run_on_pars '
            )
            for par in run_on_pars:
                msg += f'{par} '
            msg = msg.strip()
            logger.log(level=CustomLogger.ALWAYS_PRINT_LVL, msg=msg)

        logger.log(level=CustomLogger.ALWAYS_PRINT_LVL, msg=f'Start time:   {startTimeStr}')
        logger.log(level=CustomLogger.ALWAYS_PRINT_LVL, msg=f'Run on Host:  {socket.getfqdn()}')
        logger.log(level=CustomLogger.ALWAYS_PRINT_LVL, msg=f'In directory: {os.getcwd()}')

        mail_header = (
            f'Running {program_name}\n'
            f'For {core_root.stem}_{branch} -- {product_name}\n'
            f'\n'
        )

        mail_footer = (
            f'\n'
            f'------------------------------\n'
            f'\n'
            f'Ran:          {program_name}\n'
            f'At:           {startTimeStr}\n'
            f'By user:      {pwd.getpwuid(os.getuid())[0]}\n'
            f'On host:      {socket.getfqdn()}\n'
            f'In directory: {os.getcwd()}\n'
            f'\n'
            f'Full command line is:\n'
            f'\t{cmd_line_txt}\n'
        )

        env_vars = os.environ.copy()

        if not report_dir.exists():
            report_dir.mkdir(parents=True, exist_ok=True)

        with cd(results_path.resolve(), logger):
            # Get Keiko path used for this regression
            keiko_path = Path()
            keiko_settings_file = Path(f'{core_root.resolve()}/core/core_te/reglist/include_files/keiko_settings.inc')
            logger.debug(f'Extracting Keiko path from {keiko_settings_file.resolve()}')
            with keiko_settings_file.open() as keiko_fin:
                for line in keiko_fin:
                    if 'COHO_PATH' in line:
                        coho_args = line.split()
                        for arg in coho_args:
                            if 'COHO_PATH' in arg:
                                logger.debug(f'Extracting Keiko path from the following argument:\n\t{arg}')
                                coho_arg = arg.split('=')
                                keiko_path = Path(coho_arg[1]).parent
            logger.debug(f'keiko_path = {keiko_path.resolve()}')

            try:
                #step = 'lnc' if any(x in stepping.lower() for x in ['lnc', 'cgc',]) else 'pnc'
                step = stepping.split('-')[0]
                keiko_cmd = f'/usr/intel/bin/tcsh {CONST.SCRIPTS_DIR}/run_keiko_cmp.{step} --keiko_path {keiko_path.resolve()}'
                #runCmd(env_vars, keiko_cmd, logger)
            except RunError as e:
                logger.error(f'Failed to run Keiko comparison:\n\t{e}\n\tContinuing...')

        reports_cmds = []

        indicators_cmds = create_indicators_cmds(run_on_pars, fsdb_src_area, report_dir, logger)
        reports_cmds.extend(indicators_cmds)

        hierarchical_summary_cmds = create_hierarchical_summary_cmds(run_on_pars, fsdb_src_area, report_dir, logger)
        reports_cmds.extend(hierarchical_summary_cmds)

        runInPools(env_vars, reports_cmds, logger)

        if ('lnc' not in stepping) and ('cgc' not in stepping):
            mosaic_compare_cmd = create_mosaic_compare_cmd(fsdb_src_area, report_dir, product_name, branch,
                                  argv.mapping_file, argv.meta_cols, argv.metric_col, argv.rtl_domain_col, argv.coho_domain_col,
                                  argv.pass_indicators, logger)
            try:
                pass
                runCmd(env_vars, mosaic_compare_cmd, logger, log_success=True)
            except RunError as proc_error:
                logger.error('Mosaic data generation is non-gating, continuing...')

        zipped_files = roll_up_indicators(env_vars, report_dir, core_root, product_name, logger)


        if not no_checkin:
            # TODO: Replace this hardcoding with command line switches
            upload_to_power_fsdb   = True
            upload_to_power_portal = True

            powerportal_url = None

            report_area_root = Path(fsdb_dst_area / 'system_data/fsdb')
            report_area = Path(report_area_root / f'{str(core_root.name)}' / f'{product_name}')

            if upload_to_power_fsdb:
                par_name_map = Path('./par_name_map')  # Create a file containing all partition names, 1 per line, no extra newline at the end
                logger.debug(f'Creating par_name_map: {str(par_name_map.resolve())}')
                with par_name_map.open('w') as fout:
                    newline = ''  # Do not create a newline before the first entry
                    for par in run_on_pars:
                        fout.write(f'{newline}{par}')
                        newline = '\n'
                logger.debug(f'Copying results from {fsdb_src_area.resolve()} to {fsdb_dst_area.resolve()}')
                checkin_cmd = create_checkin_cmd(fsdb_src_area, core_root, fsdb_dst_area, par_name_map.resolve(), cores, logger)
                runCmd(env_vars, checkin_cmd, logger, log_success=True)
                logger.debug(f'Removing par_name_map: {str(par_name_map.resolve())}')
                par_name_map.unlink(missing_ok=True)

                same_same = compare_fsdb_src_area_to_fsdb_dst_area_and_del(fsdb_src_area, fsdb_dst_area, error_mail_list, mail_header, mail_footer, no_delete, core_root, branch, product_name, logger)

                top_ten = list()
                test_name = 'alloc_stall_atom_idle'

                #previous_report_area = get_first_previous_ww(report_area.parent, logger)
                keiko2rtl_file = Path(report_area / f'keiko2rtl.csv')
                report_area_results = Path(report_area / 'power_artist_results')
                report_area_indicators = Path(report_area / 'power_artist_indicators')
                top_ten_alloc_stall_atom_idle = report_top_ten(report_area_results, test_name, logger)

                top_ten_txt_filename = f'top_ten.{test_name}.txt'
                top_ten_txt_file = Path(f'/tmp/{top_ten_txt_filename}')
                with top_ten_txt_file.open('w') as fout:
                    fout.write(top_ten_alloc_stall_atom_idle['msg'])

                top_ten_csv_filename = f'top_ten.{test_name}.csv'
                top_ten_csv_file = Path(f'/tmp/{top_ten_csv_filename}')
                with top_ten_csv_file.open('w') as fout:
                    fout.write(top_ten_alloc_stall_atom_idle['csv'])

                copy_top_ten = f'/p/mpg/proc/common2/utils/tool_utils/archive_ci/latest/client.tcl rcp {top_ten_txt_file.resolve()} {report_area_indicators}; /p/mpg/proc/common2/utils/tool_utils/archive_ci/latest/client.tcl rcp {top_ten_csv_file.resolve()} {report_area_indicators}'
                logger.debug(f'copy_top_ten command = {copy_top_ten}')
                runCmd(env_vars, copy_top_ten, logger)

                top_ten_txt_file.unlink()
                top_ten_csv_file.unlink()

                top_ten.append(top_ten_alloc_stall_atom_idle['msg'])

            if upload_to_power_portal:
                # Upload Hierarchical Summaries to PowerPortal & Get URL of Report
                # Assume core_root is of the form:
                #   /p/hdk/rtl/models/xhdk74/pnc/core/core-pnc-a0-master-23ww22a
                # Then we can get the ip name (core) from the 1st hyphenated field of the basename
                # And we can get the workweek from the last hyphenated field of the basename
                core_root_list = core_root.name.split('-')
                ip = core_root_list[0]
                ww = core_root_list[-1]
                # Assume stepping is of the form:
                #  pnc-a0
                # Then we can get the product from the first hyphenated field of the step
                # And we can get the stepping from the last hyphenated field of the step
                stepping_list = stepping.split('-')
                product = stepping_list[0]
                step    = stepping_list[-1]
                powerportal_url = upload_to_powerportal(report_area, ip, product, step, branch, ww, product_name, logger)

            mail_indicators(core_root, mail_footer, indicators_mail_list, product_name, report_area, keiko2rtl_file, top_ten_txt_filename, top_ten, zipped_files, powerportal_url, logger)

        else:
            logger.debug(f'--no_checkin specified.\n\tNot copying results from {fsdb_src_area.resolve()} to {fsdb_dst_area.resolve()}')
            if not no_delete:
                msg = (
                    f'Even though --no_delete was not specified on the command line, keeping:\n'
                    f'\t  {fsdb_src_area.resolve()}\n'
                    f'\tsince it was not copied.  If you wish to delete this directory, please do so manually:\n'
                    f'\t\t/usr/bin/rm -rf {fsdb_src_area.resolve()}'
                )
                logger.log(level=CustomLogger.ALWAYS_PRINT_LVL, msg=msg)

    except CmdLineError as e:
        logger.fatal('Command line argument missing and could not be extracted from the environment')
        logger.fatal(f'{e}')
        exit_error(logger)
    except Exception as e:
        logger.fatal(f'Uncaught exception: {e}')
        logger.fatal(traceback.format_exc())
        exit_error(logger)

    current_time = time.localtime()
    run_finish_time = datetime.datetime.now().replace(microsecond=0)
    endTimeStr = time.strftime('%a %d-%b-%Y %H:%M:%S', current_time)
    logger.log(level=CustomLogger.ALWAYS_PRINT_LVL, msg=f'Exiting:  {program_name}')
    logger.log(level=CustomLogger.ALWAYS_PRINT_LVL, msg=f'End time: {endTimeStr}')
    logger.log(level=CustomLogger.ALWAYS_PRINT_LVL, msg=f'Total run time: {run_finish_time - run_start_time}')

    # Close the logfile associated with the logger
    CustomLogger.close_handlers(logger)

    return return_code

@contextmanager
def cd(newdir: Path, logger: logging.Logger):
    prevdir = Path(os.getcwd())
    logger.info(f'cd(): Changing to directory:\n\t{newdir.resolve()}')
    os.chdir(newdir.resolve())
    try:
        yield
    finally:
        os.chdir(prevdir.resolve())
        logger.info(f'cd(): Changing to directory:\n\t{prevdir.resolve()}')


if __name__ == '__main__':
    return_code = main()
    sys.exit(return_code)
