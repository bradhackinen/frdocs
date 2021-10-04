import os
from pathlib import Path
from msgpack import load,dump
import regex as re
from tqdm import tqdm
import pandas as pd

import frdocs
from frdocs.config import data_dir


def cfr_dict_to_string(d):
    if d['chapter']:
        return '%s CFR Chapter %s %s' % (d['title'], d['chapter'], d['part'])
    else:
        return '%s CFR %s' % (d['title'], d['part'])


def clean_info(record):
    record = record.copy()

    # Rename document_number --> frdoc_number (less ambigous when combining with other data)
    record['frdoc_number'] = record['document_number']

    # Classify type and reprint, correction, temporary using document number and action
    record['fr_type'] = record['type']
    record['is_reprint'] = 'R' in record['frdoc_number']
    record['is_correction'] = bool(re.search(r'[CXZ]', record['frdoc_number'])) | bool(record['correction_of'])
    record['is_temporary'] = False

    if record['action']:
        action = record['action'].lower()

        if re.search(r'proposed\s+rule', action):
            if re.search(r'advance.+notice.+proposed\s+rule', action):
                record['type'] = 'ANPRM'
            else:
                record['type'] = 'Proposed Rule'

        elif re.search(r'final\s+rule', action) or record['fr_type'] == 'Rule':
            if 'interim' in action and 'affirmation' not in action:
                record['type'] = 'Interim Rule'
            elif re.search('(affirmation.+rule|notice of effective date)', action):
                record['type'] = 'Affirmation of Rule'
            elif re.search(r'(direct|immediate|emergency)', action) \
                    and 'withdrawal' not in action:
                record['type'] = 'Direct Rule'
            else:
                record['type'] = 'Rule'

        elif re.search(r'regulatory\sagenda', action):
            record['type'] = 'Regulatory Agenda'

        if re.search(r'(exten(d|sion)|reopening).+(time|period|deadline)', action):
            if 'comment' in action:
                record['type'] = 'Comment Extension'
            elif 'filing' in action:
                record['type'] = 'Filing Extension'

        if bool(re.search(r'correcti(on|ng)', action)):
            record['is_correction'] = True

        if 'temporary' in action:
            record['is_temporary'] = True

    # Assume documents with missing 'significant' record are not significant
    record['significant'] = record['significant'] or False

    record['ult_agency_ids'] = tuple(
        sorted({frdocs.agency_ult_parent[agency['id']]
                for agency in record.get('agencies', []) if 'id' in agency}))

    record['ult_agencies'] = tuple(frdocs.agency_id_translator[i] for i in record['ult_agency_ids'])

    record['agency_ids'] = tuple(sorted({agency['id'] for agency in record.get('agencies', []) if 'id' in agency}))
    record['agencies'] = tuple(frdocs.agency_id_translator[i] for i in record['agency_ids'])
    record['agencies_short'] = tuple(frdocs.agency_short[i] for i in record['agency_ids'])

    record['cfr_references'] = [cfr_dict_to_string(d) for d in record.get('cfr_references', [])]

    if record['pdf_url']:
        record['text_url'] = record['pdf_url'].replace('/pdf/', '/html/').replace('.pdf', '.htm')

    return record


def main(args):
    print('Compiling cleaned info from metatadata')

    if not os.path.isdir(Path(data_dir) / 'info'):
        os.makedirs(Path(data_dir) / 'info')

    # Read daily metadata files
    index = []
    for file in tqdm(os.listdir(Path(data_dir) / 'raw' / 'meta')):
        with open(Path(data_dir) / 'raw' / 'meta' / file, 'rb') as f_read:
            records = load(f_read)

            for record in records:

                # clean all fields in each record
                record = clean_info(record)

                # Save each record in info folder
                with open(Path(data_dir) / 'info' / f"{record['frdoc_number']}.msgpack", 'wb') as f_write:
                    dump(record,f_write)

                # Save print info to index
                index.append({k:record[k] for k in ['frdoc_number','publication_date','volume','start_page','end_page','fr_type']})

    index_df = pd.DataFrame(index)
    index_df.to_csv(Path(data_dir)/'index.csv',index=False)

    print(f"Compiled info for {len(index_df)} printed documents ({index_df['frdoc_number'].nunique()} unique frdocs)")


if __name__ == '__main__':
    main()
