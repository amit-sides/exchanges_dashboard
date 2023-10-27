#!/usr/bin/env python

"""
Copied from https://github.com/anki-code/metabase-sql-wrapper
"""
import subprocess
import signal
import os


class Process:
    def __init__(self, cmd):
        self.cmd = cmd
        self.pid = None
        self.proc = None
        self.stopped = False
        for s in [signal.SIGINT, signal.SIGTERM]:
            signal.signal(s, self.proc_terminate)

    def run(self):
        proc = subprocess.Popen(self.cmd, shell=True)
        self.proc = proc
        self.pid = proc.pid
        proc.wait()

    def proc_terminate(self, signum, frame):
        print(f'*** CATCH: signum={signum}, stopping the process...')
        if self.proc:
            self.proc.terminate()
            self.stopped = True

def main():
    print('*** Metabase SQL wrapper [https://github.com/anki-code/metabase-sql-wrapper]')

    metabase_jar = '/app/metabase.jar'
    metabase_db_path = os.getenv('MB_DB_FILE', '/data/metabase')
    metabase_db_path = os.path.expanduser(metabase_db_path)

    metabase_db_path_exists = os.path.exists(metabase_db_path)
    if metabase_db_path_exists:
        print(f'*** Metabase DB path: {metabase_db_path}')
    else:
        os.makedirs(metabase_db_path)
        print(f'*** Metabase DB path created: {metabase_db_path}')

    metabase_db_file = os.path.join(metabase_db_path, os.path.basename(metabase_db_path))

    init_sql_file = os.getenv('MB_DB_INIT_SQL_FILE')
    if init_sql_file and os.path.exists(init_sql_file):
        if metabase_db_path_exists:
            print(f'*** Database path {metabase_db_path} exists, SKIP creating database from {init_sql_file}')
        else:
            print(f'*** Create database {metabase_db_file} from {init_sql_file}')
            subprocess.run(f'java -cp {metabase_jar} org.h2.tools.RunScript -url jdbc:h2:{metabase_db_file} -script {init_sql_file}', shell=True)
            print('*** Creating DONE')
    else:
        print(f'*** MB_DB_INIT_SQL_FILE {init_sql_file} not found, SKIP')

    p = Process('/app/run_metabase.sh')
    p.run()

    save_sql_file = os.getenv('MB_DB_SAVE_TO_SQL_FILE')
    if save_sql_file:
        print(f'*** Saving database {metabase_db_file} to {save_sql_file}')
        subprocess.run(f'java -cp {metabase_jar} org.h2.tools.Script -url jdbc:h2:{metabase_db_file} -script {save_sql_file}', shell=True)
        print('*** Saving DONE')


if __name__ == '__main__':
    main()
