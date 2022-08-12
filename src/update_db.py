import os
import sys
import MySQLdb
from typing import List, Tuple


# these environment variables are needed
try:
    DB_HOST = os.environ['DB_HOST']
    DB_USER = os.environ['DB_USER']
    DB_PASS = os.environ['DB_PASS']
    DB_ROOT_USER = os.environ['DB_ROOT_USER']
    DB_ROOT_USER_PASS = os.environ['DB_ROOT_USER_PASS']

except KeyError as e:
    sys.stderr.write(f'\n [ERROR] Env variable {e} must be set to run this script\n\n')
    exit(1)


def init_db(cursor):
    """Initialize the database and create tables (if not already exist)."""

    cursor.execute('create database if not exists iptoas')
    cursor.execute('use iptoas')
    cursor.execute("""
    create table if not exists routerviews (
        ip_router varchar(255),
        noderouter int,
        mask int,
        primary key (ip_router, mask)
    )
    """)
    cursor.execute("""
    create table if not exists namenodes (
        noden int,
        name varchar(255),
        primary key (noden, name)
    )
    """)


def parse_prefix_to_asn_mapping(file_name:str):
    """Parse the file containing IP prefixes and the corresponding ASN.
    The file must have one prefix per line in the format `<IP>/<MASK> <ASN>
    """

    with open(file_name, 'rt') as fp:
        data = fp.read()

    # we expect each line in the file to have a prefix to ASN map
    data = data.strip().split('\n')

    mapping = []
    for line in data:
        try:
            # each line has the pattern like: '1.0.0.0/24 13335'
            prefix, asn = line.split(' ')
            ip, mask = prefix.split('/')

            mapping.append((ip, int(asn), int(mask)))
        except Exception as e:
            raise ValueError(f'Failed parsing line "{line}"\nReason: {e}')

    return mapping


def write_prefix_mappings(mappings:List[Tuple[int, str, int]]):
    """Write the prefix to ASN mappings obtained from `parse_prefix_to_asn_mapping`
    into the MySQL database."""

    with MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS) as conn:
        with conn.cursor() as c:
            init_db(cursor=c)

            n = c.executemany('insert into routerviews (ip_router, noderouter, mask) values (%s, %s, %s)', mappings)
            conn.commit()
            return n


def parse_asn_names_mapping(file_name:str) -> List[Tuple[int, str]]:
    """Parse the file that contains the mappings between the ASN and the organization
    names. The file is expected to have one line per ASN with the format `<ASN>\t<NAME>.
    For example:

    ```
    1   FOO
    2   BAR
    ```
    """

    with open(file_name, 'rt') as fp:
        data = fp.read()

    # we expect each line in the file to have an ASN and its name
    data = data.strip().split('\n')

    names = []
    for line in data:
        try:
            # each line has the pattern like: '<ASN>\t<NAME>'
            asn, org_name = line.split('\t')

            names.append((int(asn), org_name))
        except Exception as e:
            raise ValueError(f'Failed parsing line "{line}"\nReason: {e}')

    return names


def write_asn_name(mappings:List[Tuple[int, str]]):
    """Write the ASN to organization name mappings parsed with `parse_asn_names_mapping`
    into the MySQL database."""

    with MySQLdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS) as conn:
        with conn.cursor() as c:
            init_db(cursor=c)

            n = c.executemany('insert into namenodes (noden, name) values (%s, %s)', mappings)
            conn.commit()
            return n

def give_permission(user:str):
    with MySQLdb.connect(host=DB_HOST, user=DB_ROOT_USER, passwd=DB_ROOT_USER_PASS) as conn:
        conn.query("GRANT ALL PRIVILEGES ON *.* TO '{}'@'%'".format(user))


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--asn_prefix_file_name', help='File with the ASN prefixes')
    parser.add_argument('--asn_to_name_file_name', help='File with the ASN to AS name mapping')

    args = parser.parse_args()

    give_permission('tix')

    if args.asn_prefix_file_name:
        mappings = parse_prefix_to_asn_mapping(file_name=args.asn_prefix_file_name)
        n = write_prefix_mappings(mappings=mappings)
        print(' > Table `routerviews` updated successfully.', 'Wrote', n, 'entries.')
    
    if args.asn_to_name_file_name:
        mappings = parse_asn_names_mapping(file_name=args.asn_to_name_file_name)
        n = write_asn_name(mappings=mappings)
        print(' > Table `namenodes` updated successfully.', 'Wrote', n, 'entries.')