#!/usr/bin/env python3

import shutil
import sys
import argparse
import os.path
import pathlib
import datetime
import typing

import dateutil.parser
import sqlalchemy
import sqlalchemy.orm

from scraper_root.scraper.persistence.orm_classes import (_DECL_BASE, OrderEntity, DailyBalanceEntity, BalanceEntity,
                                                          AssetBalanceEntity, PositionEntity, CurrentPriceEntity,
                                                          IncomeEntity, TradeEntity, TradedSymbolEntity,
                                                          SymbolCheckEntity)
ENTITY_TO_DATE_FIELD = {
    OrderEntity: lambda table: table.registration_datetime,
    DailyBalanceEntity: lambda table: table.day,
    BalanceEntity: lambda table: table.registration_datetime,
    AssetBalanceEntity: lambda table: table.registration_datetime,
    PositionEntity: lambda table: table.registration_datetime,
    CurrentPriceEntity: lambda table: table.registration_datetime,
    IncomeEntity: lambda table: table.time,
    TradeEntity: lambda table: table.time,
    TradedSymbolEntity: lambda table: table.registration_datetime,
    SymbolCheckEntity: lambda table: table.registration_datetime,
}

BACKUP_FORMAT = ".backup.{}"
BACKUP_REGEX = "*.backup.[0-9]*"


def parse_args():
    parser = argparse.ArgumentParser(
        prog=sys.argv[0],
        description='Removes old records from DB',
        epilog='If you get a readonly database error, try running with sudo')
    parser.add_argument("db_path", metavar="<DB Path>", type=str,
                        help="The path to the database file. Example: ./data/exchanges_db.sqlite")
    parser.add_argument("account", metavar="<Account>", type=str,
                        help="The account to delete records for. Use 'all' to delete records from all accounts.")
    parser.add_argument("-b", "--before_date", dest="before", metavar="<Before Date>",
                        type=lambda d: dateutil.parser.parse(d).date(),
                        help="Deletes all records before this date.")
    parser.add_argument('-a', "--after_date", dest="after", metavar="<After Date>",
                        type=lambda d: dateutil.parser.parse(d).date(),
                        help="Deletes all records after this date.")
    args = parser.parse_args()
    return args


def create_backup(db_path: str) -> str:
    backup_format = db_path + BACKUP_FORMAT
    backup_number = 0
    while True:
        backup_number += 1
        backup_path = backup_format.format(backup_number)
        if not os.path.exists(backup_path):
            break

    shutil.copyfile(db_path, backup_path)
    return backup_path


def clean_backups(db_path: str) -> typing.List[pathlib.Path]:
    old_backups = []
    db_dir = os.path.dirname(db_path)
    for db_backup in pathlib.Path(db_dir).rglob(BACKUP_REGEX):
        if not db_backup.is_file():
            continue
        db_modification_time = datetime.datetime.fromtimestamp(db_backup.stat().st_mtime)
        if db_modification_time < datetime.datetime.now() - datetime.timedelta(days=30):
            old_backups.append(db_backup)
    return old_backups


def create_session(db_path: str):
    abs_path = os.path.abspath(db_path)
    engine = sqlalchemy.create_engine(url=f"sqlite:///{abs_path}")
    _DECL_BASE.metadata.create_all(engine)
    session_maker = sqlalchemy.orm.sessionmaker(bind=engine)
    return session_maker


def yes_or_no(question):
    answer_format = " Y[es]/N[o]/A[ll]: "
    answer = input(question + answer_format).lower().strip()
    while answer != "y" and answer != "yes" and \
            answer != "n" and answer != "no" and \
            answer != "a" and answer != "all":
        print("Input yes or no or all:")
        answer = input(question + answer_format).lower().strip()
    if answer[0] == "a":
        return True, True
    if answer[0] == "y":
        return True, False
    return False, False


def delete_records(session: sqlalchemy.orm.Session, table: type(_DECL_BASE), account: str,
                   before: datetime.date, after: datetime.date, yes_all: bool = False) -> bool:
    print("====================================================")
    query = session.query(table)
    date_field = ENTITY_TO_DATE_FIELD[table](table)
    if before:
        query = query.filter(date_field < before)
    if after:
        query = query.filter(date_field > after)
    if account != "all":
        query = query.filter(table.account == account)

    records_count = query.count()
    if records_count == 0:
        print(f"No records to delete from {table.__tablename__}.")
        return yes_all
    print(f"Deleting {records_count} records from table '{table.__tablename__}'.")

    yes = yes_all
    if not yes_all:
        yes, yes_all = yes_or_no(f"{query.count()} records will be deleted. Continue?")
    if yes:
        query.delete()
        session.commit()
    return yes_all


def main():
    args = parse_args()
    if not os.path.isfile(args.db_path):
        raise ValueError(f"Database path '{args.db_path}' doesn't exist!")

    if args.before is None and args.after is None:
        raise ValueError("Must set either '-a' or '-b' !")

    if args.before and args.after and args.after > args.before:
        raise ValueError("'after_date' must be earlier than 'before_date' !")

    account = args.account.strip()
    session_maker = create_session(args.db_path)
    with session_maker() as session:
        if account != "all":
            distinct_accounts = session.query(BalanceEntity.account).distinct().all()
            accounts = [account[0] for account in distinct_accounts]
            if account not in accounts:
                raise ValueError(f"Account '{account}' doesn't exist in database! Existing accounts: {accounts}")

        backup_db = create_backup(args.db_path)
        print(f"Created backup of database file at '{backup_db}")

        query = "Delete all records  "
        if account != "all":
            query += f"for account '{account}', "
        if args.before:
            query += f"before '{args.before}', "
        if args.after:
            query += f"after '{args.after}', "
        print(f"Given query: {query[:-2]}")

        all_tables = [OrderEntity, DailyBalanceEntity, BalanceEntity, AssetBalanceEntity, PositionEntity,
                      CurrentPriceEntity, IncomeEntity, TradeEntity, TradedSymbolEntity, SymbolCheckEntity]
        auto_delete = False
        for table in all_tables:
            auto_delete = delete_records(session, table, account, args.before, args.after, auto_delete)

    old_backups = clean_backups(args.db_path)
    if len(old_backups) > 0:
        delete, _ = yes_or_no("Old backups detected, delete them?")
        if delete:
            for old_backup in old_backups:
                old_backup.unlink()


if __name__ == "__main__":
    main()
