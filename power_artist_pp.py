#!/usr/intel/bin/python3.11.1

'''
* ============================================================================
* INTEL TOP SECRET
*
* Copyright (c) 2020-2022 Intel Corporation All Rights Reserved.
*
* The source code contained or described herein and all documents related to
* the source code ("Material") are owned by Intel Corporation or its
* suppliers or licensors.
*
* Title to the Material remains with Intel Corporation or its suppliers and
* licensors. The Material contains trade secrets and proprietary and
* confidential information of Intel or its suppliers and licensors.
* The Material is protected by worldwide copyright and trade secret laws and
* treaty provisions. No part of the Material may be used, copied, reproduced,
* modified, published, uploaded, posted, transmitted, distributed,
* or disclosed in any way without Intel's prior express written permission.
*
* No license under any patent, copyright, trade secret or other intellectual
* property right is granted to or conferred upon you by disclosure
* or delivery of the Materials, either expressly, by implication, inducement,
* estoppel or otherwise. Any license under such intellectual property rights
* must be express and approved by Intel in writing.
*
* = PRODUCT
*      Core PnP
*
* = FILENAME
*      power_artist_pp.py
*
* = DESCRIPTION
*      Script to run the pre-stages to run Power Artist and then run Power Artist
*      on all specified partitions
*
* = AUTHOR
*      Rob Slater (rob.slater@intel.com)
*
* = CREATION DATE
*      7 April 2022
* ============================================================================
'''

import argparse
import datetime
import inspect
import io
import logging
import math
import os
import pwd
import shutil
import socket
import subprocess
import sys
import time
import traceback

from contextlib import contextmanager
from distutils.dir_util import copy_tree
from multiprocessing import Pool
from pathlib import Path
from typing import List, Tuple
from utils import get_diff_reference_release, get_partitions
import CustomLogger


# Initialize constants used in this script
class CONST(object):
    __slots__ = ()  # Eliminate the ability to change the object dictionary after instantiation
    KB2GB = 1048576  # (1,024)^2
    KB2MB = 1024

    STR_ENCODING = 'utf-8'

    POWER_FSDB_CUT_CMD = '/nfs/site/disks/core.trace.repo/bin/power_artist_pp/dev/power_fsdb_pp.pl'
    SCRIPTS_DIR         = os.path.dirname(os.path.realpath(__file__))
    PA_HIER_SUMMARY     = os.path.join(SCRIPTS_DIR, 'gen_power_artist_hierarchical_summary.py')
    DIFF_POWER = os.path.join(SCRIPTS_DIR, 'diff_power')
    RELEASE_AREA        = '/p/pnc/power_central_dir/power/system_data/fsdb'
    POWER_FSDB_WORK_DIR = 'power_fsdb_pp'

    GK_FACELESS_ACCOUNT = 'cnladmin'  # Account which runs Gatekeeper turnins


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


def rm_tree(directory: Path) -> None:
    ''' Recursively remove a directory tree for Python versions < 3.6 where this is not built into Pathlib'''
    for child in directory.iterdir():
        if child.is_file() or child.is_symlink():
            child.unlink()
        else:
            rm_tree(child)
    directory.rmdir()


def string_num_to_int(num: str, logger: logging.Logger) -> int:
    """ Convert abbreviate number into int.
        Such that 1.5M will be 15000000

    Args :
        num (str) : abbreviated number
        logger (logging.Logger) : logger handle

    Returns:
        actual_num (int) : Long form of the number
    """
    method_name = 'string_num_to_int'
    logger.debug(f'Entering: {method_name}')

    suffixes = {'k': 3,
                'm': 6,
                'b': 9
                }

    suffix = num[-1].lower()
    if suffix not in suffixes:
        if suffix.isdigit():
            actual_num = int(num, 0)
            logger.debug(f'Converted {num} to {actual_num:,}')
            logger.debug(f'Exiting: {method_name}')
            return actual_num
        else:
            logger.error(f'{num} is not supported')
            logger.debug(f'Exiting: {method_name}')
            return -1
    else:
        digit_part = 1
        if len(num) == 1:
            digit_part = int(1)
        else:
            digit_part = int(float(num[:-1]))
        actual_num = digit_part * (10 ** suffixes[suffix])
        logger.debug(f'Converted {num} to {actual_num:,}')
        logger.debug(f'Exiting: {method_name}')
        return actual_num


def human_format(num: int, precision: int = 2, suffixes: str = None) -> str:
    if suffixes is None:
        suffixes = ['', 'K', 'M', 'G', 'T', 'P']
    m = 0 if num == 0 else int(math.log10(num) // 3)
    return f'{num / 1000.0 ** m:.{precision}f}{suffixes[m]}'


def get_script_dir(follow_symlinks=True) -> str:
    """ Gets the location that this Python script was called from """
    if getattr(sys, 'frozen', False):  # py2exe, PyInstaller, cx_Freeze
        path = os.path.abspath(sys.executable)
    else:
        path = inspect.getabsfile(get_script_dir)
    if follow_symlinks:
        path = os.path.realpath(path)
    return os.path.dirname(path)


class FileLock:
    '''Get and release a lock on the results area so shared files can be safely updated'''

    def __init__(self, report_dir: Path, logger: logging.Logger) -> None:
        self.logger = logger
        self.unlocked = True
        hostname = socket.getfqdn()
        pid = os.getpid()
        self.host_pid = f'{hostname}_{pid}'
        self.report_dir = report_dir
        self.lock_file = Path(self.report_dir / '.lock')

    def lock(self) -> None:
        '''This method will block until a lock is obtained'''

        while self.unlocked:
            sleep_cnt = 0
            while self.lock_file.exists():
                sleep_cnt += 1
                if sleep_cnt % 5:
                    self.logger.debug(f'Waiting for {str(self.lock_file)} to be removed')
                time.sleep(2)
            try:
                with self.lock_file.open('w') as fout:
                    fout.write(self.host_pid)
            except Exception as e:
                self.logger.debug(f'Got the following error while writing to the lock file:\n\t{e}')
                continue
            else:
                data = ''
                try:
                    with self.lock_file.open('r') as fin:
                        data = fin.readline()
                except Exception as e:
                    self.logger.debug(f'Got the following error while reading from the lock file:\n\t{e}')
                    continue
                else:
                    data = data.strip()
                    if data == self.host_pid:
                        self.unlocked = False  # We successfully captured the directory lock
                        self.logger.debug(f'Successfully locked {str(self.report_dir)} with {str(self.lock_file)}')
                    else:
                        self.logger.debug(f'Someone else locked {str(self.report_dir)} with {str(self.lock_file)}')
        return

    def unlock(self) -> bool:
        '''Remove the report_dir lock.  Returns True if unlocked, False otherwise'''
        if self.unlocked:
            if not self.lock_file.exists():
                self.logger.debug(f'Called unlocked when we were already unlocked; nothing to do here')
        elif not self.lock_file.exists():
            self.logger.error(
                f'We thought we had a lock, but {str(self.lock_file)} does not exist.  Releasing internal state')
            self.unlocked = True
        else:  # We think we own the lock and the lock file exists.  Make sure we really have the lock file, and if so, delete it
            data = ''
            try:
                with self.lock_file.open('r') as fin:
                    data = fin.readline()
            except Exception as e:
                self.logger.error(f'Could not read in {str(self.lock_file)}')
            else:
                data = data.strip()
                if data == self.host_pid:
                    try:
                        self.lock_file.unlink()
                    except Exception as e:
                        self.logger.error(f'Could not delete our own lock file ({str(self.lock_file)}\n\t{e}')
                        # self.unlocked stays False
                    else:
                        self.unlocked = True
                else:
                    self.logger.error(
                        f'We ({self.host_pid}) thought we had the {str(self.lock_file)}, but in fact {data} does\n\tOoops... releasing the internal state lock')
                    self.unlocked = True
        return self.unlocked


def generate_report(test_name: str, output_dir: Path, power_artist_dir: Path, report_dir: Path, core_root: Path,
                    stepping: str, run_on_pars: List, cmd_line_txt: str, pa_die: str, logger: logging.Logger) -> None:
    '''Copy the Power Reports to the archive for this regression'''
    # Create a directory with the test name and then copy:
    #  The PA reports (maybe just the BLA.csv file)
    #  The power_fsdb_pp reports
    #  The Eff line from logbook.log so the test can be re-run
    #  the power_artist.py command

    if not report_dir.exists():
        report_dir.mkdir(parents=True, exist_ok=True)

    # PowerArtist report file we are interested in:
    #  output_dir/<die>/<par>/pa_power/reducepower/<test_name>/<par>_<par>/report/<par>.red.cge.seq.csv
    #  output_dir/<die>/<par>/pa_power/reducepower/<test_name>/<par>_<par>/report/<par>.red_cg.rpt
    #  output_dir/<die>/<par>/pa_power/reducepower/<test_name>/<par>_<par>/report/<par>.red.rpt_ref
    #  output_dir/<die>/<par>/pa_power/reducepower/<test_name>/<par>_<par>/report/<par>.red.bar.csv
    # Example:
    #  power_artist/server/par_fe/pa_power/reducepower/quicktrace_glc_smt2/par_fe_par_fe/report/par_fe.red.cge.seq.csv
    #  power_artist/server/par_fe/pa_power/reducepower/quicktrace_glc_smt2/par_fe_par_fe/report/par_fe.red_cg.rpt
    #  power_artist/server/par_fe/pa_power/reducepower/quicktrace_glc_smt2/par_fe_par_fe/report/par_fe.red.rpt_ref
    #  power_artist/server/par_fe/pa_power/reducepower/quicktrace_glc_smt2/par_fe_par_fe/report/par_fe.red.bar.csv
    pa_results_dir = Path(report_dir / 'power_artist_results')
    pa_results_dir.mkdir(parents=True, exist_ok=True)

    pa_test_results_dir = Path(pa_results_dir / test_name)
    if pa_test_results_dir.exists():
        rm_tree(pa_test_results_dir)
    pa_test_results_dir.mkdir()

    pa_cmd_file = Path(pa_test_results_dir / 'power_artist_pp.py.cmd')
    with pa_cmd_file.open('w') as fout:
        fout.write(f'{cmd_line_txt}\n')

    pa_hier_summary_cmds = []
    for par in run_on_pars:
        pa_report_src = Path(output_dir / f'{pa_die}' / par / 'pa_power' / 'reducepower' / test_name / f'{par}_{par}' / 'report')
        pa_csv_report_src = Path(pa_report_src / f'{par}.red.cge.seq.csv')
        pa_rpt_report_src = Path(pa_report_src / f'{par}.red_cg.rpt')
        pa_ref_report_src = Path(pa_report_src / f'{par}.red.rpt_ref')
        pa_bar_csv_report_src = Path(pa_report_src / f'{par}.red.bar.csv')
        pa_csv_report_dst = Path(pa_test_results_dir / f'{par}.red.cge.seq.csv')
        pa_rpt_report_dst = Path(pa_test_results_dir / f'{par}.red_cg.rpt')
        pa_ref_report_dst = Path(pa_test_results_dir / f'{par}.red.rpt_ref')
        pa_bar_csv_report_dst = Path(pa_test_results_dir / f'{par}.red.bar.csv')
        try:
            logger.debug(f'Generate Report: Copying from {pa_csv_report_src} to {pa_csv_report_dst}')
            shutil.copy2(str(pa_csv_report_src), str(pa_csv_report_dst))
        except FileNotFoundError as e:
            logger.warning(f'File: {str(pa_csv_report_src)} not found, could not be copied:\n\t{e}')
        try:
            logger.debug(f'Generate Report: Copying from {pa_ref_report_src} to {pa_ref_report_dst}')
            shutil.copy2(str(pa_rpt_report_src), str(pa_rpt_report_dst))
        except FileNotFoundError as e:
            logger.warning(f'File: {str(pa_rpt_report_src)} not found, could not be copied:\n\t{e}')
        try:
            shutil.copy2(str(pa_ref_report_src), str(pa_ref_report_dst))
        except FileNotFoundError as e:
            logger.warning(f'File: {str(pa_ref_report_src)} not found, could not be copied:\n\t{e}')
        try:
            shutil.copy2(str(pa_bar_csv_report_src), str(pa_bar_csv_report_dst))
        except FileNotFoundError as e:
            logger.warning(f'File: {str(pa_bar_csv_report_src)} not found, could not be copied:\n\t{e}')
        try:
            shutil.copy2(f'{str(output_dir)}/../*.stats', f'{str(report_dir)}')
        except FileNotFoundError as e:
            logger.warning(f'File: {str(output_dir)}/../*.stats not found, could not be copied:\n\t{e}')
        try:
            if Path(f'{str(output_dir)}/../{test_name}_edp_0.csv').exists():
                logger.debug(f'Renaming {str(output_dir)}/../{test_name}_edp_0.csv to {str(output_dir)}/../{test_name}.edp_0.csv')
                shutil.move(f'{str(output_dir)}/../{test_name}_edp_0.csv', f'{str(output_dir)}/../{test_name}.edp_0.csv')
            shutil.copy2(f'{str(output_dir)}/../{test_name}.edp_0.csv', f'{str(report_dir)}')
        except FileNotFoundError as e:
            logger.warning(f'File: {str(output_dir)}/../{test_name}.edp_0.csv not found, could not be copied:\n\t{e}')
        try:
            shutil.move(f'{str(output_dir)}/../specman.pt', f'{str(output_dir)}/../{test_name}.specman.pt')
            shutil.copy2(f'{str(output_dir)}/../*.pt', f'{str(report_dir)}')
        except FileNotFoundError as e:
            logger.warning(f'File: {str(output_dir)}/../*.pt not found, could not be copied:\n\t{e}')
        try:
            shutil.move(f'{str(pa_report_src)}/rtl_line_pwr_sorted.rpt', f'{str(pa_report_src)}/{par}.rtl_line_pwr_sorted.rpt')
            shutil.copy2(f'{str(pa_report_src)}/{par}.rtl_line_pwr_sorted.rpt', f'{str(pa_test_results_dir)}')
        except FileNotFoundError as e:
            logger.warning(f'File: {str(pa_report_src)}/rtl_line_pwr_sorted.rpt not found, could not be copied:\n\t{e}')
        try:
            shutil.move(f'{str(pa_report_src)}/rtl_line_pwr_sorted.csv', f'{str(pa_report_src)}/{par}.rtl_line_pwr_sorted.csv')
            shutil.copy2(f'{str(pa_report_src)}/{par}.rtl_line_pwr_sorted.csv', f'{str(pa_test_results_dir)}')
        except FileNotFoundError as e:
            logger.warning(f'File: {str(pa_report_src)}/rtl_line_pwr_sorted.csv not found, could not be copied:\n\t{e}')
        try:
            shutil.move(f'{str(pa_report_src)}/RTLPwrPerLine.csv', f'{str(pa_report_src)}/{par}.RTLPwrPerLine.csv')
            shutil.copy2(f'{str(pa_report_src)}/{par}.RTLPwrPerLine.csv', f'{str(pa_test_results_dir)}')
        except FileNotFoundError as e:
            logger.warning(f'File: {str(pa_report_src)}/RTLPwrPerLine.csv not found, could not be copied:\n\t{e}')

        # Copy one test's wmtrun.log file, so it can be used to open Verdi waveforms with rtldbug
        # It doesn't matter which test, so we chose alloc_stall_atom_idle since it is probably the smallest
        env_vars = os.environ.copy()
        if 'alloc_stall_atom_idle' in test_name:
            logger.debug(f'Copying {str(output_dir)}/../wmtrun.log* to {str(report_dir)}')
            cmd = f'/usr/intel/bin/gzip -9v {str(output_dir)}/../wmtrun.log*'
            try:
                runCmd(env_vars, cmd, logger)
                shutil.copy2(f'{str(output_dir)}/../wmtrun.log.gz', f'{str(report_dir)}')
            except (RunError, FileNotFoundError) as e:
                logger.warning(f'File: {str(output_dir)}/../wmtrun.log* not found, could not be copied:\n\t{e}')

        pa_hier_test_summary_dst = Path(pa_test_results_dir / f'{par}_pa_hier_summary.csv')
        cmd = (
            f'{CONST.PA_HIER_SUMMARY}'
            f' --power_artist_results {str(pa_results_dir)}'
            f' --out {str(pa_hier_test_summary_dst)}'
            f' --partition {par}'
            f' --tests {test_name}'
        )
        logger.debug(f'Created command:\n\t{cmd}')
        pa_hier_summary_cmds.append(cmd)
    
    diff_ref_release = get_diff_reference_release(CONST.RELEASE_AREA, stepping, logger)
    pa_diff_cmd = []
    if diff_ref_release:
        release_name = Path(diff_ref_release).name
        diffs_dir = Path(report_dir / f'diff_vs_{release_name}')
        cmd = (
            f'{CONST.DIFF_POWER}'
            f' --results1 {str(diff_ref_release)}'
            f' --tag1 {release_name}'
            f' --results2 {str(report_dir.parent)}'
            f' --tag2 {core_root.name}'
            f' --diffs_dir {str(diffs_dir)}'
        )
        pa_diff_cmd.append(cmd)

    results = runInPools(env_vars, pa_hier_summary_cmds, logger)
    for result in results:
        msg = (
            f'Power Artist results:\n'
            f'\t{result[1].get()}'
        )
        logger.debug(msg)
    
    if pa_diff_cmd:
        env_vars = os.environ.copy()
        results = runInPools(env_vars, pa_diff_cmd, logger)
        for result in results:
            msg = (
                f'Power Artist results:\n'
                f'\t{result[1].get()}'
            )
            logger.debug(msg)
    return
        
def running_in_tmp(logger: logging.Logger) -> bool:
    ''' Detect if we are running from /tmp '''
    # If we are working in /tmp (e.g. a Gatekeeper regression) then do not
    # farm out power_fsdb_pp.pl script to Netbatch (the NB target won't see the local
    # /tmp directory
    in_tmp = (os.getcwd()[0:5] == '/tmp/')
    logger.debug(f'running_in_tmp() returning {in_tmp}')
    return in_tmp


def main() -> int:
    # Determine start time and save so total run time can be calculated later
    current_time = time.localtime()
    run_start_time = datetime.datetime.now().replace(microsecond=0)
    startTimeStr = time.strftime('%a %d-%b-%Y %H:%M:%S', current_time)

    args, fsdb_pp_script_args = parse_fsdb_pp_script_args(sys.argv)

    argv = parse_args(args)

    program_name = os.path.basename(__file__)

    console = None
    if not argv.quiet:
        console = sys.stdout

    logfile = argv.logfile if Path(argv.logfile).suffix == '.log' else f'{argv.logfile}.log'
    logger = CustomLogger.create_logger(program_name, logfile, argv.verbosity.upper(), console)

    try:
        # TODO 22ww38: Once enough weeks pass, argv.fsdb_pp_script can be made required=True, pp_script can be removed,
        #       this code block can be deleted, and the create_fsdb_pp_cmd below can be called with argv.fsdb_pp_script
        if not (argv.pp_script or argv.fsdb_pp_script):
            msg = f'Either --fsdb_pp_script or --pp_script (deprecated) must be supplied on the command line'
            raise CmdLineError(msg)
        fsdb_pp_script = argv.fsdb_pp_script if argv.fsdb_pp_script else argv.pp_script

        core_root = argv.core_root if argv.core_root else os.environ.get('MODEL_ROOT')
        if not core_root:
            msg = '--core_root not specified on command line and could not extract via $MODEL_ROOT, exiting...'
            raise CmdLineError(msg)
        else:
            core_root = Path(core_root)

        dut = argv.dut if argv.dut else os.environ.get('DUT')
        if dut is None:
            raise CmdLineError(f'--dut was not supplied on the command line and $DUT is not set')

        die = argv.die_name if argv.die_name else os.environ.get('DIE_NAME')
        if die is None:
            raise CmdLineError(f'--die_name was not supplied on the command line and $DIE_NAME is not set')

        product_name = argv.product_name

        stepping = argv.stepping if argv.stepping else os.environ.get('STEPPING')
        if stepping is None:
            raise CmdLineError(f'--stepping was not supplied on the command line and $STEPPING is not set')

        branch = argv.branch if argv.branch else os.environ.get('branch_is')
        if branch is None:
            raise CmdLineError(f'--branch was not supplied on the command line and $branch_is is not set')

        test_directory = Path(argv.test_directory) if argv.test_directory else Path(os.getcwd())
        if not test_directory.exists():
            raise CmdLineError(f'--test_directory argument specifies a directory that does not exist')

        test_name = argv.test_name if argv.test_name else os.environ.get('TESTNAME')
        if test_name is None:
            raise CmdLineError(f'--test_name was not supplied on the command line and $TESTNAME is not set')

        run_on_pars = get_partitions(core_root, product_name, logger)
        if argv.run_on_pars:
            run_on_pars = argv.run_on_pars if 'all' not in argv.run_on_pars else run_on_pars

        instruction_start = string_num_to_int(argv.instruction_start, logger)
        instruction_end   = string_num_to_int(argv.instruction_end,   logger)

        output_dir = Path(argv.output_dir)  # Default value in argparse is os.getcwd()/power_artist
        report_dir = output_dir  # If neither --report_dir nor $RESULTS_PATH is set, use the output dir
        if argv.report_dir:
            report_dir = Path(argv.report_dir)
        elif os.environ.get('RESULTS_PATH') is not None:
            if product_name:
                report_dir = Path(f'{os.environ.get("RESULTS_PATH")}/pa_results_{core_root.stem}/{product_name}')
            else:
                report_dir = Path(f'{os.environ.get("RESULTS_PATH")}/pa_results_{core_root.stem}/{die}')

        # To prevent overriding, report_dir should contain the core_root name and product_name (if available,
        # otherwise the die name)
        # Make the modification now, so it appears in the command line output, but for log readability, report later that
        # we made this change
        report_msg = None
        report_dir_new = report_dir
        if core_root.stem not in str(report_dir):
            report_dir_new = report_dir / f'{core_root.stem}_{branch}'
        if product_name and product_name not in str(report_dir_new):
            report_dir_new = report_dir_new / product_name
        elif die not in str(report_dir_new):
            report_dir_new = report_dir_new / die
        if str(report_dir) != str(report_dir_new):
            report_msg = (
                f'The --report_dir argument was modified to include model and product names\n'
                f'\tWas: {str(report_dir)}\n'
                f'\tNow: {str(report_dir_new)}'
            )
            report_dir = report_dir_new

        wave_replay_path = Path(argv.wave_replay_path) if argv.wave_replay_path else Path(
            core_root / 'core/common/bin/wave_replay.tcsh')
        cmd_line_txt = gen_cmd_line(core_root, output_dir, report_dir, run_on_pars, stepping, die, branch,
                                    test_directory, test_name, wave_replay_path, logger)

        if argv.model_run_failed:
            logger.log(level=CustomLogger.ALWAYS_PRINT_LVL,
                       msg='Test failed the MODEL RUN stage.  Will not automatically run Power Artist')
            logger.log(level=CustomLogger.ALWAYS_PRINT_LVL,
                       msg=f'If you would like to manually run Power Artist anyway, run the following command:\n\t{cmd_line_txt}')
            logger.log(level=CustomLogger.ALWAYS_PRINT_LVL, msg='Exiting...')

            # Close the logfile associated with the logger
            CustomLogger.close_handlers(logger)
            return 0

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
        logger.log(level=CustomLogger.ALWAYS_PRINT_LVL, msg=f'Start time: {startTimeStr}')
        logger.log(level=CustomLogger.ALWAYS_PRINT_LVL, msg=f'Run on Host: {socket.getfqdn()}')
        logger.log(level=CustomLogger.ALWAYS_PRINT_LVL, msg=f'In directory: {os.getcwd()}')

        # Print out message that we modified --report_dir as per above
        if report_msg:
            logger.info(report_msg)

        # If a product name is given on the command line (e.g. --product_name core_clientm_n3) use this instead
        # of the die name.  Since all the code below is coded to use die, just overwrite the die value with product_name
        if product_name:
            die = product_name
            logger.debug(
                f'--product_name was used on the command line.  Using {product_name} as the die name instead of {die}')

        # Power Artist Cheetah wrapper die names do not include the "core_" prefix.
        # So if the die name starts with "core_" then remove it to get the Power Artist Die name.
        # Examples:
        #   core_client --> client
        #   core_server_1276 --> server_1276
        pa_die = die
        if pa_die.startswith('core_'):
            pa_die = pa_die[len('core_'):]
        if pa_die != die:
            logger.debug(
                f'Converted --die_name or --product_name argument ({die}) to Power Artist die naming scheme: {pa_die}')

        env_vars = os.environ.copy()

        if not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)

        # The Power Artist run itself is large (up to 40GB of memory) and there
        # is one Power Artist run per PAR.  Thus, on a typical regression, around
        # dozen Power Artist jobs need to be run (they are parallelized since one
        # PAR does not affect another)
        #
        # This clearly cannot be done on the regression host and rather needs to
        # be farmed out to larger Netbatch machines (one per job).
        #
        # If we are working in /tmp (e.g. a Gatekeeper regression) the target
        # the Power Artist job gets sent to will not see the locally created
        # data (/tmp is only mounted on the local machine)
        #
        # In this case, the Power Artist input data generated by the power_fsdb_pp.pl
        # script needs to be on a NFS mounted disk so any server in the pool can see it
        #
        # In the end, the cut FSDBs and PA results will be placed in the directory
        # supplied by report_dir, which should be an NFS mounted disk.  So we will
        # at the start place the cut FSDBs and other generated collateral there

        power_artist_dir = Path(output_dir)

        if not power_artist_dir.exists():
            power_artist_dir.mkdir()

        logger.debug(f'fsdb_pp_script_args = {fsdb_pp_script_args}')

        if not argv.skip_fsdb_pp_script:
            report_dir.mkdir(parents=True, exist_ok=True)
            fsdb_pp_cmd = create_fsdb_pp_cmd(fsdb_pp_script, stepping, fsdb_pp_script_args,
                                             instruction_start, instruction_end,
                                             test_directory, test_name,
                                             power_artist_dir, report_dir, logger)
            runCmd(env_vars, fsdb_pp_cmd, logger)

            logger.debug(
                f'Successfully created reduced FSDBs and window sizing information in {str(power_artist_dir)}/{CONST.POWER_FSDB_WORK_DIR}')
            logger.debug(f'Creating a Power Artist workarea for each PAR in {str(power_artist_dir)}')
        else:
            logger.info(f'--skip_fsdb_pp_script used.  Assuming cut FSDBs already appear in {str(report_dir)}')

        if argv.only_run_fsdb_pp_script:
            logger.info(f'--only_run_fsdb_pp_script used.  Not running PowerArtist')
        else:
            if argv.skip_power_artist:
                logger.info(f'--skip_power_artist used.  Not running PowerArtist')
            else:
                logger.info(f'Building and running Power Artist commands...')
                master_test_list = create_power_artist_WA(core_root, stepping, test_name, power_artist_dir, report_dir, run_on_pars, logger)
                cmd = 'sync;sync;sync'
                runCmd(env_vars, cmd, logger)

                pa_cmds = create_pa_cmd(core_root, dut, pa_die, run_on_pars, master_test_list, output_dir, logger,
                                        argv.pa_run_script, test_name, core_root.stem, argv.verbosity)
                results = runInPools(env_vars, pa_cmds, logger)

                for result in results:
                    msg = (
                        f'Power Artist results:\n'
                        f'\t{result[1].get()}'
                    )
                    logger.debug(msg)

            generate_report(test_name, output_dir, power_artist_dir, report_dir, core_root, stepping, run_on_pars, cmd_line_txt, pa_die, logger)

            if argv.delete_output_dir:
                msg = (
                    f'--delete_output_dir used on the command line\n'
                    f'\tRemoving {str(output_dir)}'
                )
                logger.debug(msg)
                try:
                    rm_tree(output_dir)
                except Exception as e:
                    logger.error(f'Could not delete Power Artist working directory:\n\t{str(output_dir)}\n\t{e}')
                else:
                    logger.debug(f'Successfully removed {str(output_dir)}')

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

    return 0


def gen_cmd_line(core_root: Path, output_dir: Path, report_dir: Path, run_on_pars: List, stepping: str, die: str,
                 branch: str, test_directory: Path, test_name: str, wave_replay_path: Path,
                 logger: logging.Logger) -> str:
    msg = ''
    for argv_arg in sys.argv:
        msg += f'{argv_arg} '
    if '--core_root ' not in msg:
        msg += f'--core_root {core_root.resolve()} '
    if '--stepping' not in msg:
        if stepping:
            msg += f'--stepping {stepping} '
    if '--die_name' not in msg:
        if die:
            msg += f'--die_name {die} '
    if '--branch' not in msg:
        if branch:
            msg += f'--branch {branch} '
    if '--test_name' not in msg:
        msg += f'--test_name {test_name} '
    if '--run_on_pars' not in msg:
        msg += '--run_on_pars '
        for par in run_on_pars:
            msg += f'{par} '
    if '--output_dir' not in msg:
        msg += f'--output_dir {str(output_dir)} '
    if '--report_dir' not in msg:
        msg += f'--report_dir {str(report_dir)} '
    if '--test_directory' not in msg:
        msg += f'--test_directory {str(test_directory)} '
    if '--test_name' not in msg:
        msg += f'--test_name {test_name} '
    if '--wave_replay_path' not in msg:
        msg += f'--wave_replay_path {str(wave_replay_path)} '

    msg = msg.strip()
    logger.debug(f'Generated command line is:\n\t{msg}')

    return msg


def create_power_artist_WA(core_root: Path, stepping: str, test_name: str, power_artist_dir: Path, report_dir: Path, run_on_pars: List,
                           logger: logging.Logger) -> Path:

    power_fsdb_pp_work_dir = Path(power_artist_dir / CONST.POWER_FSDB_WORK_DIR)

    core_list = {0: 'icore0', 1: 'icore1',}
    core_num  = 0

    # cat pp_work_dir/window_times.txt
    # 1, 47404372ps, 48559372ps, 25000, 250ps
    # Window start is element 1
    # Window stop  is element 2

    window_times = Path(power_fsdb_pp_work_dir / 'window_times.txt')
    if not window_times.exists():
        msg = f'FSDB cut information is missing.  Cannot find file:\n\t{str(window_times)}'
        logger.error(msg)
        raise RunError(msg)

    window_times_list = list()
    with window_times.open() as fin:
        line = fin.readline().strip()
        window_times_list = line.split(',')
        for index in range(len(window_times_list)):
            window_times_list[index] = window_times_list[index].strip()

    start = window_times_list[1]
    stop = window_times_list[2]

    # Create two Master Test List files:
    #  (1) A file with all the tests and all the partitions.
    #      This will be sent as input to Prime Power in the backend flow
    #  (2) A file per test which contains all the partitions for that test only
    #      Even though the Cheetah wrapper gets passed in both the test
    #      and partition to be run, it does not select by test but rather
    #      tries to run on all tests which match the specified partition.
    #      The workaround to this is to separate the tests into separate
    #      Master Test List files.  Now there will only be one partition
    #      match per test
    master_test_list = Path(report_dir / f'master_test_list.{test_name}')
    master_test_list_regression = Path(report_dir / 'master_test_list')

    fsdb_location = Path(report_dir / f'{test_name}.fsdb')
    # par_exe:nhmpkg.core_top.core.par_exe:par_exe::exe3:/p/lnc/power_central_dir/power/system_data/fsdb/core-lnc-a0-22ww14a_akoushik/exe3.fsdb:::49028372ps:50123872ps

    filelock = FileLock(report_dir, logger)

    filelock.lock()
    with master_test_list.open('a') as mtl, master_test_list_regression.open('a') as mtlr:
        # Example format of Power Artist master_test_list entry:
        # par_exe:nhmpkg.core_top.core.par_exe:par_exe::exe3:/p/lnc/power_central_dir/power/system_data/fsdb/core-lnc-a0-22ww14a_akoushik/exe3.fsdb:::49028372ps:50123872ps
        for par in run_on_pars:
            prefix = 'nhmpkg.core_top.core'
            if 'lnc' not in stepping and 'cgc' not in stepping:
                sys.path.append(f'{core_root.resolve()}/tools/suchef')
                from suchef_restructure_config import suchef_restrucutre_config_by_top

                if par in suchef_restrucutre_config_by_top('icore'):
                    prefix += f'.{core_list[core_num]}'
            line = f'{par}:{prefix}.{par}:{par}::{test_name}:{str(report_dir.resolve())}/{test_name}.fsdb:{start}:{stop}\n'
            mtl.write(line)
            mtlr.write(line)
    filelock.unlock()

    return master_test_list


def create_fsdb_pp_cmd(pp_script: str, stepping: str, fsdb_pp_script_args: List,
                       instruction_start: int, instruction_end: int,
                       test_directory: Path, test_name: str,
                       power_artist_dir: Path, report_dir: Path, logger: logging.Logger) -> str:
    power_fsdb_pp_work_dir = Path(power_artist_dir / CONST.POWER_FSDB_WORK_DIR)
    if power_fsdb_pp_work_dir.exists():
        rm_tree(power_fsdb_pp_work_dir)
    power_fsdb_pp_work_dir.mkdir()

    # If we are working in /tmp (e.g. a Gatekeeper regression) then do not
    # farm out pp.pl script to Netbatch (the NB target won't see the local
    # /tmp directory
    nb = '' if running_in_tmp(logger) else '-nb '

    # Either we were passed in --fsdb_pp_script_args <args> --fsdb_pp_script_args-- which will contain the cut window
    # or we can derive it from the --instruction_start --instruction_end arguments
    final_fsdb_pp_script_args = ' '.join(fsdb_pp_script_args) if fsdb_pp_script_args is not None else None
    if final_fsdb_pp_script_args is None:
        final_fsdb_pp_script_args = f'-stats -eom_s {instruction_start} -eom_e {instruction_end}'

    fsdb_pp_cmd = (
        f'{pp_script} '
        f'-stepping {stepping} '
        f'{final_fsdb_pp_script_args} '
        f'-dir {str(test_directory)} '
        f'-test {test_name} '
        f'-wa {str(power_fsdb_pp_work_dir.resolve())} '
        f'{nb}'
        f'-fsdb_in {str(test_directory.resolve())}/verilog.fsdb '
        f'-fsdb_out {str(report_dir.resolve())}/{test_name}.cut.fsdb '
        f'>& {str(power_fsdb_pp_work_dir.resolve())}/power_fsdb_pp.log.txt'
    )

    logger.debug(f'Created the following power_fsdb_pp command:\n\t{fsdb_pp_cmd}')

    return fsdb_pp_cmd


def create_pa_cmd(core_root: Path, dut: str, pa_die: str, run_on_pars: List, master_test_list: Path, output_dir: Path,
                  logger: logging.Logger, pa_run_script: Path, test_name: str, model_name: str, verbosity: str) -> List:
    pa_cmds = list()

    output_abs_dir = output_dir
    if not output_dir.is_absolute():
        output_abs_dir = output_dir.resolve(strict=True)

    for par in run_on_pars:
        # /usr/bin/time make -C $MODEL_ROOT/tools/buildflow/power/powerartist pa_report DUT=core TOP=par_exe MASTER_TEST_LIST=/nfs/site/disks/rslater_wa/pa_test/exe3 PA_MODE=reducepower OUTPUT_DIR_OVERRIDE=/nfs/site/disks/rslater_wa/pa_test

        user_name = pwd.getpwuid(os.getuid())[0]
        nb_pool = 'sc8_critical_gk' if user_name == CONST.GK_FACELESS_ACCOUNT else 'sc8_normal'
        nb_qslot = '/c2dg/core/lnc/lnca/fe/gk/all/regress' if user_name == CONST.GK_FACELESS_ACCOUNT else 'power_cost'

        msg = (
            f'User is {user_name}, thus using:\n'
            f'\tTarget: {nb_pool}\n'
            f'\tQslot:  {nb_qslot}'
        )
        logger.debug(msg)

        # If pa_die includes the process, then separate this out into a DIE only and PROCESS only variables
        # Example
        #  pa_die = client_1278 --> DIE=client PROCESS=1278
        #
        # First, find the last underscore:
        underscore_loc = pa_die.rfind('_')
        die = pa_die
        process = ''
        if 0 < underscore_loc < len(pa_die) - 1 and not pa_die.endswith(
                'client_m'):  # Catch cases where the '_' is the 1st or last char
            # Then everything before the last underscore is the DIE name and everything after is the PROCESS name:
            die = pa_die[:underscore_loc]
            process = f'{pa_die[underscore_loc + 1:]}'
            logger.debug(f'Converting DIE_PROCESS die name ({pa_die}) to DIE={die} & PROCESS={process}')

        pa_cmd = (
            f'/usr/bin/make '
            f'-C {core_root.resolve()}/tools/buildflow/power/powerartist '
            f'pa_report '
            f'DUT={dut} '
            f'DIE={die} '
            f'PROCESS={process} '
            f'TOP={par} '
            f'TEST={test_name} '
            f'MASTER_TEST_LIST={str(master_test_list.resolve())} '
            f'PA_MODE=reducepower '
            f'OUTPUT_DIR_OVERRIDE= '  # Will be filled in by the target with the target's full path to output_dir
            f'> {output_dir}/power_artist_{par}.log.txt'
        )

        # akoushik comment: Getting the hostname of the nb machine, to be passed as an argument to the pp_run_script. This will be used to rsync/scp the data back to the host when the PA runs completes.
        hostname = socket.getfqdn()

        # akoushik comment: Below are the command line options to pp_run_script:
        # 1. --host_dir: This takes the path of the Target A machine where the TREX is run in this format - <hostname>:<full_path_of_output_directory>
        # 2. --test_name: Takes the test name of the current Trex run, which is further used in the script
        # 3. --par: The current partition that the PA needs to be run on
        # 4. --power_artist_cmd: The above prepared PA command line
        host = ''
        nfsdir = ''
        if running_in_tmp(logger):
            host = f'--host_dir {hostname}:{os.getcwd()}/{output_dir}'
        else:
            nfsdir = f'--nfsdir {str(Path(output_abs_dir).parent)}'  # The output_dir will be specified as its argument below
        nb_mem = '48G' if 'par_ooo_int' in par else '32G'
        cmd_t = f'/usr/intel/bin/nbjob run --target {nb_pool} --qslot {nb_qslot} --class "{nb_mem}&&1C&&SLES12" --mode interactive-ls {pa_run_script} {host}{nfsdir} --test_name {test_name} --power_artist_cmd \"{pa_cmd}\" --par {par} --test_name {test_name} --model_name {model_name} --user_name {user_name} --verbosity {verbosity} --output_dir_override {output_dir}'

        logger.debug(f'Created Power Artist command line:\n\t{cmd_t}')
        pa_cmds.append(cmd_t)

    logger.debug(f'Created {len(pa_cmds)} Power Artist commands to run on all specified PARs')

    return pa_cmds


def parse_args(argv: List) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Process test output for power and run Power Artist')
    parser.add_argument('-cr', '--core_root', type=str, default=f'{os.environ.get("MODEL_ROOT")}',
                        help='Location of model being run, defaults to $MODEL_ROOT')
    parser.add_argument('-d', '--dut', type=str, help='DUT to run power analysis on (Default: The TREX $DUT variable)')
    parser.add_argument('-die', '--die_name', type=str,
                        help='DIE to run power analysis on (Default: The TREX $DIE_NAME variable)')
    parser.add_argument('-prod', '--product_name', type=str,
                        help='Product name to run on.  If set, will override --die_name the argument')
    parser.add_argument('-s', '--stepping', type=str, help='Core step to run the power analysis on')
    parser.add_argument('-b', '--branch', type=str, help='Name of the git Cluster branch we are running on')
    parser.add_argument('-td', '--test_directory', type=str, help='Directory test was run from (Default: $CWD)')
    parser.add_argument('-tn', '--test_name', type=str,
                        help='Name of the TREX test run (Default: The TREX $TESTNAME setting')
    parser.add_argument('-r', '--run_on_pars', type=str, nargs='*',
                        help='Which partitions to run Power Artist analysis on (Default: All partitions in the project')
    parser.add_argument('-l', '--logfile', type=str, default=f'{os.path.basename(sys.argv[0])}.log',
                        help=f'Log file to use (Default: {os.path.basename(sys.argv[0])}.log')
    parser.add_argument('-o', '--output_dir', type=str, default=f'{os.getcwd()}/power_artist',
                        help=f'Directory to run in (Default: Create a directory called "power_artist" in the directory this script is run from')
    parser.add_argument('-del', '--delete_output_dir', action='store_true',
                        help=f'Delete the Power Artist working directory (--output_dir) after finished running PA.  Should be used with --report_dir or all data will be lost')
    parser.add_argument('-rp', '--report_dir', type=str,
                        help=f'Central repository to copy results to (typically only used in regressions).  Default is $RESULTS_DIR, if set otherwise the setting for --output_dir')
    parser.add_argument('-v', '--verbosity', type=str,
                        choices=['notset', 'debug', 'info', 'warning', 'error', 'critical'], default='warning',
                        help='Log verbosity level (default: warning)')
    parser.add_argument('-q', '--quiet', action='store_true', help='Do not print output to the console')
    parser.add_argument('-p', '--fsdb_pp_script', type=str, required=False,
                        help='Location of the Power Artist pre-processing script power_fsdb_pp.pl')
    parser.add_argument('-pold', '--pp_script', type=str, required=False,
                        help='Deprecated: Location of the Power Artist pre-processing script pp.pl')
    parser.add_argument('-pa', '--fsdb_pp_script_args', type=str, nargs='*',
                        help='Arguments to be passed to the Power Artist pre-processing script power_fsdb_pp.pl')
    parser.add_argument('-paold', '--pp_script_args', type=str, nargs='*',
                        help='Deprecated: Arguments to be passed to the Power Artist pre-processing script pp.pl')
    parser.add_argument('-oc', '--only_run_fsdb_pp_script', action='store_true', help='Only cut the FSDB, do not run Power Artist')
    parser.add_argument('-n', '--pa_run_script', type=str,
                        default='/nfs/site/disks/core.trace.repo/bin/power_artist_pp/lnc/run_PA.py', required=False,
                        help='Location of the Power Artist run script - run_PA.py ')
    parser.add_argument('-sfsdb', '--skip_fsdb_pp_script', action='store_true',
                        help="Don't call the power_fsdb_pp.pl script to cut the FSDB (assumption is this was called in a previous run and now the User only wants to re-run Power Artist")
    parser.add_argument('-spa', '--skip_power_artist', action='store_true',
                        help="Don't run PowerArtist (assumes you have saved power_artist data and just want to run post-processing")
    parser.add_argument('-f', '--model_run_failed', action='store_true',
                        help='Test failed, so do not expect data to be sane')
    parser.add_argument('-w', '--wave_replay_path', type=str,
                        help='Location of wave_replay Synopsys IDX FSDB cutting script (Default: $MODEL_ROOT/core/common/bin/wave_replay.tcsh)')
    parser.add_argument('-is',    '--instruction_start', type=str, default='0',
                        help='Instruction retirement to start analysis.  Only used if --fsdb_pp_script_args is not supplied.  DEFAULT: Start of trace')
    parser.add_argument('-ie',    '--instruction_end',   type=str, default=f'{sys.maxsize}',
                        help='Instruction retirement to end analysis.  Only used if --fsdb_pp_script_args is not supplied.  DEFAULT: End of trace')

    return parser.parse_args(argv)


def parse_fsdb_pp_script_args(args: List) -> Tuple[List, List]:
    # Before handing the commandline arguments to argparser, search out the fsdb_pp_script args:
    #   --fsdb_pp_script_args <fsdb_pp_script_args> --fsdb_pp_script_args--
    # Because argparse cannot handle this style of command line arguments
    # Once the fsdb_pp_script args are saved off, pass off the rest of the command line arguments
    # (without the actual name of the program being called) to parse_args() as usual

    # TODO 22ww38: Remove --pp_script_args and only use --fsdb_pp_script_args
    pp_start = False
    pp_stop = False
    fsdb_pp_script_args = list()
    argv_new = list()
    for arg in args:
        if arg == '--fsdb_pp_script_args' or arg == '--pp_script_args':
            pp_start = True
        elif arg == '--fsdb_pp_script_args--' or arg == '--pp_script_args--':
            pp_stop = True
        elif not pp_start and not pp_stop:  # Before we reached --fsdb_pp_script_args
            argv_new.append(arg)
        elif pp_start and not pp_stop:  # We are in between --fsdb_pp_script_args and --fsdb_pp_script_args--, save off the argument
            fsdb_pp_script_args.append(arg)
        elif pp_start and pp_stop:  # After we reached --fsdb_pp_script_args--
            argv_new.append(arg)

    if len(fsdb_pp_script_args) == 0:
        fsdb_pp_script_args = None

    argv_new = argv_new[1:]  # Chop off the first argument, it is the command we are running

    return argv_new, fsdb_pp_script_args


def exit_error(logger: logging.Logger) -> None:
    '''Make sure logging handlers are flushed & closed and then exit with the specified error'''
    CustomLogger.close_handlers(logger)
    sys.exit(1)


def runCmd(env_vars: dict, cmd: str, logger: logging.Logger) -> str:
    if env_vars is None:
        env_vars = os.environ.copy()

    logger.debug(f'Running command:\n\t{cmd}')

    try:
        return_code = subprocess.run(cmd, shell=True, capture_output=True, check=True, env=env_vars)
    except subprocess.CalledProcessError as proc_error:
        msg = (f'Command:\n\t{proc_error.cmd}\nreturned error:\n\t{proc_error.returncode}'
               f'\n\tstdout:\t{proc_error.stdout.decode(CONST.STR_ENCODING)}'
               f'\n\tstderr:\t{proc_error.stderr.decode(CONST.STR_ENCODING)}'
        )

        logger.error(msg)
        raise RunError(msg)
    else:
        msg = f'Command {cmd} returned successfully'
        logger.debug(msg)

    return msg


def runInPools(env_vars: dict, cmds: List, logger: logging.Logger) -> List:
    logger.debug(f'Running {len(cmds)} commands in parallel')

    results = []
    with Pool(processes=len(cmds)) as pool:
        for cmd in cmds:
            logger.debug(f'Calling run_cmd({cmd} , logger')
            result = pool.apply_async(runCmd, [env_vars, cmd, logger])
            results.append([cmd, result])

        logger.debug(f'Created {len(cmds)} pools')
        pool.close()
        logger.debug(f'Waiting for {len(cmds)} pools to finish')
        pool.join()
        logger.debug(f'{len(cmds)} pools finished')

    return results


@contextmanager
def cd(newdir: str, logger: logging.Logger):
    prevdir = os.getcwd()
    logger.info(f'cd(): Changing to directory:\n\t{newdir}')
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        os.chdir(prevdir)


if __name__ == '__main__':
    return_code = main()
    sys.exit(return_code)
