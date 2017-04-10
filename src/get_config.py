import os.path
try:
    import configparser
except ImportError:
    from six.moves import configparser

DEFAULT_LOGIN = {
    'host': 'localhost',
    'port': '3306',
    'user': 'ec2-user'
}


def get_config(filename=None):
    filename = os.path.expanduser(filename or "~/.my.cnf")
    cp = configparser.ConfigParser(defaults=DEFAULT_LOGIN)
    with open(filename) as f:
        try:
            cp.read_file(f)
        except AttributeError:
            cp.readfp(f)

        if cp.has_section('client'):
            return {
                "host": cp.get('client', 'host'),
                "passwd": cp.get('client', 'password'),
                "port": int(cp.get('client', 'port')),
                "user": cp.get('client', 'user'),
            }
        else:
            return {}
