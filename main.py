from collections import defaultdict
from csv import DictReader
from json import dump
from json import load
from os.path import exists
from subprocess import PIPE
from subprocess import Popen
from time import sleep

class IDCache(defaultdict):
    '''A dict-like object with patron statuses (e.g. 'undergraduate') as keys
    and a list of netids as values.
    '''
    def __init__(self):
        super().__init__(list)
        self.FP = './id_cache.json'
        if exists(self.FP):
            self._load()

    def includes(self, id):
        '''Returns True if this id in _any_ of the dict entries
        '''
        return any([id in self[k] for k in self.keys()])

    def patron_type(self, id):
        for k in self.keys():
            if id in self[k]:
                return k # will exit loop

    def _dump(self):
        with open(self.FP, 'w') as f:
            dump(self, f, ensure_ascii=False, indent=2)

    def _load(self):
        with open(self.FP, 'r') as f:
            for k,v in load(f).items():
                self[k] = v

    def build(self, report_path, dump_every=200):
        '''Build the cache. Hits LDAP once for each unknown (to us) ID. Note
        that the cache will be initialzed with the entries in ./id_cache.json
        if it exists, so calling this method will only hit LDAP for new IDs if
        this has been run before.
        '''
        filtr = lambda r: r['Cancelled At'] != '' or r['Location'] != 'Test Branch'
        with open(pth) as csv:
            reader = DictReader(csv)
            c = 0
            for reservation in filter(filtr, reader):
                id = reservation['Email'].split('@')[0]
                if not self.includes(id):
                    patron_type = IDCache.get_patron_type(id)
                    print(f'Adding {id} to cache')
                    self[patron_type].append(id)
                    # Note that None/null (when serialized) may be one of the keys.
                    # Also above, superclass defaultdict(list) with create a list
                    # if a key is not already initialized.
                    c+=1
                    if c % dump_every == 0:
                        self._dump()
                        self.clear()
                        self._load()
                # else:
                #     print(f'{id} in cache')
        self._dump()


    @staticmethod
    def get_patron_type(id):
        query = IDCache._build_query(id, 'uid')
        patron_type = IDCache._run_query(query)
        if patron_type is None:
            # might be an email alias
            query = IDCache._build_query(id, 'mail')
            patron_type = IDCache._run_query(query)
        return patron_type

    @staticmethod
    def _build_query(id, field):
        # Build the elements of an ldapsearch command
        filter = f'{field}={id}'
        if field == 'mail':
            filter = f'{filter}@princeton.edu'
        return ['ldapsearch', '-h', 'ldap.princeton.edu', '-p', '389', '-x', '-b', 'o=Princeton University,c=US', filter]

    @staticmethod
    def _run_query(q):
        # Shell out to ldapsearch. Right now just returns a string with the
        # status, but the intermediate dict could be useful in the future.
        proc = Popen(q, stderr=PIPE, stdout=PIPE)
        stdout, stderr = proc.communicate()
        d = {}
        for line in stdout.decode('utf-8', errors='ignore').strip().split("\n"):
            if ':' in line:
                tokens = line.split(':')
                k = tokens[0]
                if len(tokens) == 2:
                    d[k] = tokens[1].strip()
                else:
                    d[k] = ':'.join(tokens[1:]).strip()
        return d.get('pustatus')

if __name__ == '__main__':
    pth = '/Users/jstroop/workspace/seat_analysis/seats_feb1_mar19.csv'
    cache = IDCache()
    cache.build(pth)
