# Python >= 3.6 for f-strings
from collections import defaultdict
from csv import DictReader
from csv import writer
from datetime import date
from json import dump
from json import load
from os.path import exists
from subprocess import PIPE
from subprocess import Popen
from time import sleep

class SeatDataReader():
    def report_filter(self, r):
        return (r['Cancelled At'] != '' or r['Checked In At'] != '' or r['Location'] != 'Test Branch')

    def read_report(self, pth):
        with open(pth) as csv:
            # Just take the whole thing into memory
            report = list(DictReader(csv))
        return report

class IDCache(defaultdict, SeatDataReader):
    '''A dict-like object with patron statuses (e.g. 'undergraduate') as keys
    and a list of netids as values.
    '''
    def __init__(self):
        super().__init__(list)
        self.cache_path = './id_cache.json'
        if exists(self.cache_path):
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
        with open(self.cache_path, 'w') as f:
            dump(self, f, ensure_ascii=False, indent=2)

    def _load(self):
        with open(self.cache_path, 'r') as f:
            for k,v in load(f).items():
                self[k] = v

    def build(self, report_path, dump_every=200):
        '''Build the cache. Hits LDAP once for each unknown (to us) ID. Note
        that the cache will be initialzed with the entries in ./id_cache.json
        if it exists, so calling this method will only hit LDAP for new IDs if
        this has been run before.
        '''
        try:
            report = self.read_report(report_path)
            c = 0
            for reservation in filter(self.report_filter, report):
                if not self.includes(reservation['Email']):
                    id = reservation['Email'].split('@')[0]
                    patron_type = IDCache.get_patron_type(id)
                    print(f'Adding {id} to cache')
                    self[patron_type].append(reservation['Email'])
                    c+=1
                    if c % dump_every == 0: self._dumpload()
        except:
            self._dump()
            raise
        self._dump()

    def _dumpload(self):
        self._dump()
        self.clear()
        self._load()

    @staticmethod
    def get_patron_type(id):
        query = IDCache._build_query(id, 'uid')
        patron_type = IDCache._run_query(query)
        if patron_type is None: # might be an email alias
            query = IDCache._build_query(id, 'mail')
            patron_type = IDCache._run_query(query)
        if patron_type is None: # still
            patron_type = 'unknown'
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

class DayTimeReporter(SeatDataReader):
    def __init__(self, id_cache):
        self.id_cache = id_cache
        self.data = {}
        self.json_dump_fp = './report.json'
        self.csv_dump_fp = './report.csv'
        self.days = ('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday')

    def run(self, report_path):
        report = self.read_report(report_path)
        for res in filter(self.report_filter, report):
            day = DayTimeReporter._day_from_reservation(res)
            if day not in self.data:
                self.data[day] = {}
            location = res['Location']
            if location not in self.data[day]:
                self.data[day][location] = {}
            time_block = DayTimeReporter._time_key_from_reservation(res)
            if time_block not in self.data[day][location]:
                self.data[day][location][time_block] = {}
            patron_type = self._patron_type_from_reservation(res)
            if patron_type not in self.data[day][location][time_block]:
                self.data[day][location][time_block][patron_type] = 1
            else:
                self.data[day][location][time_block][patron_type] +=1
        self.data = DayTimeReporter._sort_report(self.data)
        for i in range(0,7): # turn ints into days of the week
            day = self.days[i]
            self.data[day] = self.data.pop(i)
        self._dump('json')
        self._dump('csv')

    @staticmethod
    def _sort_report(d):
        tmp = {}
        for k, v in sorted(d.items()):
            if isinstance(v, dict):
                #recursive case
                tmp[k] = DayTimeReporter._sort_report(v)
            else:
                tmp[k] = v
        return tmp

    def _dump(self, fmt):
        if fmt == 'json':
            with open(self.json_dump_fp, 'w') as f:
                dump(self.data, f, ensure_ascii=False, indent=2)
        if fmt == 'csv':
            self._dump_csv()

    def _dump_csv(self):
        # 😒
        fields = ('Day', 'Location', 'Time Block', 'Patron Type', 'Count')
        with open(self.csv_dump_fp, 'w') as f:
            csv_writer = writer(f, dialect='excel')
            csv_writer.writerow(fields)
            for day in self.data.keys():
                for location in self.data[day].keys():
                    for time_block in self.data[day][location].keys():
                        for patron_type in self.data[day][location][time_block].keys():
                            count = self.data[day][location][time_block][patron_type]
                            line = (day, location, time_block, patron_type, count)
                            csv_writer.writerow(line)

    @staticmethod
    def _time_key_from_reservation(res):
        hour = int(res['From Time'].split(':')[0])
        if hour % 2 == 1:
            hour-=1
        return f'{str(hour).zfill(2)}:00 - {str(hour+1).zfill(2)}:59'

    @staticmethod
    def _day_from_reservation(res):
        # Uses zero-based/non-ISO, i.e. 0 = Monday, 6 = Sunday
        return date(*map(int, res['From Date'].split('-'))).weekday()

    def _patron_type_from_reservation(self, reservation):
        return id_cache.patron_type(reservation['Email'])


if __name__ == '__main__':
    pth = 'seats_feb1_mar19.csv'
    ##  Build an ID cache:
    id_cache = IDCache()
    id_cache.build(pth)
    ## Run the report:
    reporter = DayTimeReporter(id_cache)
    reporter.run(pth)
