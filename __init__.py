import os
from pathlib import Path
import pandas as pd
from msgpack import load
import re

from frdocs.config import data_dir

project_dir = Path(os.path.dirname(os.path.abspath(__file__)))

default_info_fields = ['frdoc_number', 'abstract', 'action', 'agencies_short',
                          'agency_ids', 'ult_agencies', 'ult_agency_ids',
                          'comments_close_on', 'docket_ids', 'publication_date',
                          'regulation_id_numbers', 'cfr_references', 'title', 'topics',
                          'fr_type', 'type', 'significant', 'is_correction',
                          'is_temporary', 'is_reprint']

# Load agency info
with open(Path(data_dir) / 'raw' / 'agencies.msgpack', 'rb') as f:
    agencies = load(f)

agency_id_translator = {record['id']: record['name'] for record in agencies}
agency_parent = {record['id']: record['parent_id'] for record in agencies}
agency_short = {record['id']: record['short_name'] for record in agencies}


def ult_parent_agency(agency_id):
    if agency_parent[agency_id]:
        return ult_parent_agency(agency_parent[agency_id])
    else:
        return agency_id


agency_ult_parent = {record['id']: ult_parent_agency(record['id']) for record in agencies}


def load_agency_df():
    df = pd.DataFrame(agencies)
    df['ult_agency_id'] = df['id'].apply(lambda i: agency_ult_parent[i])
    df['ult_agency'] = df['ult_agency_id'].apply(lambda i: agency_id_translator[i])

    return df


def load_index():
    return pd.read_csv(Path(data_dir)/'index.csv')


def load_info(frdoc_number):
    '''
    Loads a single cleaned metatadata record by frdoc number
    '''
    with open(Path(data_dir) / 'info' / f'{frdoc_number}.msgpack','rb') as f:
        return load(f)


def iter_info(frdoc_numbers=None, fields=default_info_fields, missing='warn'):

    assert missing in ['raise','warn','ignore']

    if frdoc_numbers is None:
        index_df = load_index()
        frdoc_numbers = index_df['frdoc_number']

    for frdoc_number in frdoc_numbers:
        try:
            with open(Path(data_dir) / 'info' / f'{frdoc_number}.msgpack','rb') as f:
                record = load(f)

            if fields != 'all':
                record = {k:record[k] for k in fields}

        except FileNotFoundError as e:
            if missing == 'raise':
                raise e
            elif missing == 'warn':
                print(f'Warning: No info file found for {frdoc_number}')

            continue

        yield record


def load_info_df(frdoc_numbers=None,fields=default_info_fields):
    return pd.DataFrame([tuple(r[k] for k in fields) for r in iter_info(frdoc_numbers=frdoc_numbers,fields='all')],
                            columns=fields)


def load_parsed(frdoc_number):
    '''
    Loads a single parsed file by FR document number.
    '''
    return pd.read_pickle(Path(data_dir) / 'parsed' / f'{frdoc_number}.pkl')


def iter_parsed(frdoc_numbers=None, missing='warn'):
    '''
    Iteratively load all parsed files, or all files in a list of document numbers.

    Warning: By default, iter_parsed skips files with missing data, so yielded
    data may not correspond exactly to passed document numbers. Can force
    raising errors by setting raise_missing=True
    '''
    assert missing in ['raise','warn','ignore']

    if frdoc_numbers is not None:
        files = (f'{d}.pkl' for d in frdoc_numbers)
    else:
        files = os.listdir(Path(data_dir) / 'parsed')

    for f in files:
        d = f.split('.', 1)[0]

        try:
            df = pd.read_pickle(Path(data_dir) / 'parsed' / f)

        except FileNotFoundError as e:
            if missing == 'raise':
                raise e
            elif missing == 'warn':
                print(f'Warning: No parsed file found for {d}')

            continue

        yield d, df


def iter_agenda(publications=None,rin_list=None):
    agenda_dir = Path(data_dir) / 'agenda'
    if not publications:
        publications = os.listdir(agenda_dir)

    if rin_list:
        rin_list = set(rin_list)

    for publication in publications:

        for rin_file in os.listdir(agenda_dir/publication):

            if rin_list:
                rin = rin_file.split('.')[0]
                if rin not in rin_list:
                    continue

            with open(agenda_dir/publication/rin_file,'rb') as f:
                yield load(f)


def load_agenda_df(publications=None,rin_list=None):
    return pd.DataFrame(list(iter_agenda(publications=publications,rin_list=rin_list)))


def load_timetable_df(publications=None,rin_list=None):
    return pd.DataFrame([dict(publication=record['publication'],rin=record['rin'],**event)
                            for record in iter_agenda(publications=publications,rin_list=rin_list)
                            for event in record.get('timetable',[])])


class CitationFinder():

    def __init__(self):
        self.index_df = load_index()

        self.index_df = self.index_df.sort_values(['volume','start_page','end_page','frdoc_number'])

        # multi_page = self.index_df['end_page'] > self.index_df['start_page']
        # self.index_df.loc[multi_page,'end_page'] = self.index_df.loc[multi_page,'end_page'] - 1

        self.volume = self.index_df['volume']
        self.start_page = self.index_df['start_page']
        self.end_page = self.index_df['end_page']

    def __call__(self,citation_string=None,volume=None,page=None,favour_first_page=True,strict_parsing=False):

        if not citation_string or (volume and page):
            raise ValueError('Must provide a citation string, or volume and page')

        if citation_string:
            if strict_parsing:
                m = re.search(r'(\d+) FR (\d+)',citation_string)
                if not m:
                    raise ValueError(f'Could not parse citation string "{citation_string}". You may want to try strict_parsing=False.')
            else:
                m = re.search(r'(\d+)[^0-9]+(\d+)',citation_string)
                if not m:
                    raise ValueError(f'Could not parse citation string "{citation_string}"')

            volume = m.group(1)
            page = m.group(2)

        volume = int(volume)
        page = int(page)

        df = self.index_df[(self.volume == volume) & (self.start_page <= page) & (self.end_page >= page)].copy()

        if len(df) > 1 and favour_first_page:
            df['first_page_match'] = (self.start_page == page)
            if df['first_page_match'].sum():
                df = df[df['first_page_match']]

        return list(df['frdoc_number'].values)


if __name__ == "__main__":

    print('Testing loaders')

    print('\nFunction: load_agency_df')
    df = load_agency_df()
    print('First row:')
    print(df.head(1).T)
    print(f'\nObservations: {len(df)}')

    print('\nFunction: load_info_df')
    df = load_info_df()
    print('First row:')
    print(df.head(1).T)
    print(f'\nObservations: {len(df)}')

    print('\nFunction: load_agenda_df')
    df = load_agenda_df()
    print('First row:')
    print(df.head(1).T)
    print(f'\nObservations: {len(df)}')

    print('\nFunction: load_timetable_df')
    df = load_timetable_df()
    print('First row:')
    print(df.head(1).T)
    print(f'\nObservations: {len(df)}')

    print('\nFunction: iter_parsed')
    d,df = next(iter_parsed())
    print('First row of first item:')
    print(df.head(1).T)

    print('\nFunction: load_parsed')
    df = load_parsed(d)
    print('First row of first item:')
    print(df.head(1).T)

    print('\nTesting CitationFinder')
    citation_finder = CitationFinder()
    print(f"{citation_finder('78 FR 41677')=}")
    print(f"{citation_finder('78 FR 41678')=}")
    print(f"{citation_finder('65 FR 4601')=}")
    print(f"{citation_finder('65 FR 4601',favour_first_page=False)=}")
