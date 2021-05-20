import os
from pathlib import Path
import pandas as pd
import regex as re
from msgpack import load

from frdocs.config import data_dir

project_dir = Path(os.path.dirname(os.path.abspath(__file__)))


default_fields = ['frdoc_number', 'abstract', 'action', 'agencies_short',
                  'agency_ids', 'ult_agencies', 'ult_agency_ids',
                  'comments_close_on', 'docket_ids', 'publication_date',
                  'regulation_id_numbers', 'cfr_references', 'title', 'topics',
                  'fr_type', 'type', 'significant', 'is_correction',
                  'is_temporary', 'is_reprint']  # ,'type_order','is_comment_extension','is_affirmation','is_direct']

# Load agency info
with open(Path(data_dir) / 'agencies.msgpack', 'rb') as f:
    agencies = load(f)

agency_id_translator = {info['id']: info['name'] for info in agencies}
agency_parent = {info['id']: info['parent_id'] for info in agencies}
agency_short = {info['id']: info['short_name'] for info in agencies}


def ult_parent_agency(agency_id):
    if agency_parent[agency_id]:
        return ult_parent_agency(agency_parent[agency_id])
    else:
        return agency_id


agency_ult_parent = {info['id']: ult_parent_agency(info['id']) for info in agencies}


def iter_doc_info(years=None, dates=None):
    for file in os.listdir(Path(data_dir) / 'meta'):
        if years is not None and int(file[:4]) not in years:
            continue
        if dates is not None and file[:10] not in dates:
            continue

        with open(Path(data_dir) / 'meta' / file, 'rb') as f:
            records = load(f)
            for record in records:
                yield record


def cfr_dict_to_string(d):
    if d['chapter']:
        return '%s CFR Chapter %s %s' % (d['title'], d['chapter'], d['part'])
    else:
        return '%s CFR %s' % (d['title'], d['part'])


def clean_info(info, fields=default_fields, to_tuple=False):
    info = info.copy()

    # Rename document_number --> frdoc_number (less ambigous when combining with other data)
    info['frdoc_number'] = info['document_number']

    # Classify type and reprint, correction, temporary using document number and action
    info['fr_type'] = info['type']
    info['is_reprint'] = 'R' in info['frdoc_number']
    info['is_correction'] = bool(re.search(r'[CX]', info['frdoc_number']))
    info['is_temporary'] = False

    if info['action']:
        action = info['action'].lower()
        if 'type' in fields:
            if re.search(r'proposed\s+rule', action):
                if re.search(r'advance.+notice.+proposed\s+rule', action):
                    info['type'] = 'ANPRM'
                else:
                    info['type'] = 'Proposed Rule'

            elif re.search(r'final\s+rule', action) or info['fr_type'] == 'Rule':
                if 'interim' in action and 'affirmation' not in action:
                    info['type'] = 'Interim Rule'
                elif re.search('(affirmation.+rule|notice of effective date)', action):
                    info['type'] = 'Affirmation of Rule'
                elif re.search(r'(direct|immediate|emergency)', action) \
                        and 'withdrawal' not in action:
                    info['type'] = 'Direct Rule'
                else:
                    info['type'] = 'Rule'

            elif re.search(r'regulatory\sagenda', action):
                info['type'] = 'Regulatory Agenda'

            if re.search(r'(exten(d|sion)|reopening).+(time|period|deadline)', action):
                if 'comment' in action:
                    info['type'] = 'Comment Extension'
                elif 'filing' in action:
                    info['type'] = 'Filing Extension'

        if bool(re.search(r'correcti(on|ng)', action)):
            info['is_correction'] = True

        if 'temporary' in action:
            info['is_temporary'] = True

    # Assume documents with missing 'significant' info are not significant
    info['significant'] = info['significant'] or False

    if 'ult_agencies' or 'ult_agency_ids' in fields:
        info['ult_agency_ids'] = tuple(
            sorted({agency_ult_parent[agency['id']]
                    for agency in info.get('agencies', []) if 'id' in agency}))

        info['ult_agencies'] = tuple(agency_id_translator[i]
                                     for i in info['ult_agency_ids'])

    if 'agencies' in fields or 'agency_ids' in fields \
            or 'agencies_short' in fields:

        info['agency_ids'] = tuple(
            sorted({agency['id'] for agency in info.get('agencies', []) if 'id' in agency}))
        info['agencies'] = tuple(agency_id_translator[i] for i in info['agency_ids'])
        info['agencies_short'] = tuple(agency_short[i] for i in info['agency_ids'])

    if 'cfr_references' in fields:
        info['cfr_references'] = [cfr_dict_to_string(d)
                                  for d in info.get('cfr_references', [])]

    if 'text_url' in fields:
        info['text_url'] = info['pdf_url'].replace('/pdf/', '/html/').replace('.pdf', '.htm')

    # if 'path' in fields:
    #     if info['full_text_xml_url']:
    #         m = re.search(r'xml(.+?)\.xml',info['full_text_xml_url'])
    #         info['path'] = m.group(1)
    #     else:
    #         info['path'] = np.nan

    if fields != 'all':
        info = {k: info[k] for k in fields}

    if to_tuple:
        return tuple(info[k] for k in fields)
    else:
        return info


def load_info_df(years=None, fields=default_fields):
    return pd.DataFrame([clean_info(info, fields, to_tuple=True)
                         for info in iter_doc_info(years=years)],
                        columns=fields)


def load_agency_df():
    df = pd.DataFrame(agencies)
    df['ult_agency_id'] = df['id'].apply(lambda i: agency_ult_parent[i])
    df['ult_agency'] = df['ult_agency_id'].apply(lambda i: agency_id_translator[i])

    return df


def iter_parsed(frdoc_numbers=None, raise_missing=False, verbose=True):
    '''
    Iteratively load all parsed files, or all files in a list of document numbers.

    Warning: By default, iter_parsed skips files with missing data, so yielded
    data may not correspond exactly to passed document numbers. Can force
    raising errors by setting raise_missing=True
    '''

    if frdoc_numbers is not None:
        files = (f'{d}.pkl' for d in frdoc_numbers)
    else:
        files = os.listdir(Path(data_dir) / 'parsed')

    for f in files:
        d = f.split('.', 1)[0]

        try:
            df = pd.read_pickle(Path(data_dir) / 'parsed' / f)
        except Exception as e:
            if raise_missing:
                raise e
            else:
                if verbose:
                    print(f'Warning: No parsed file found for {d}')
                continue

        yield d, df


def load_parsed(frdoc_number):
    '''
    Loads a single parsed file by FR document number.
    '''
    return pd.read_pickle(Path(data_dir) / 'parsed' / f'{frdoc_number}.pkl')


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

    print('\nFunction: iter_parsed')
    for d, df in iter_parsed():
        break
    print('First row of first item:')
    print(df.head(1).T)

    print('\nFunction: load_parsed')
    df = load_parsed(d)
    print('First row of first item:')
    print(df.head(1).T)
