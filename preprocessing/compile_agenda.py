import os
from pathlib import Path
import lxml.etree as et
from msgpack import dump
from tqdm import tqdm
import random

from frdocs.config import data_dir
from frdocs import CitationFinder, load_info_df


def element_to_dict(e):
    d = {}
    if len(e):
        for child in e:
            key = child.tag.lower()
            if len(child):
                d[key] = [element_to_dict(c) for c in child]
            else:
                d[key] = child.text

        return d
    else:
        return {e.tag.lower():e.text}


def clean_agenda(record,citation_finder=None):

    record = record.copy()

    record['publication'] = '-'.join([record['publication'][0]['publication_id'][:4],record['publication'][0]['publication_id'][4:]])

    # Parse HTML formatted text in abstract
    if record['abstract']:
        record['abstract'] = ''.join(et.HTML(record['abstract']).itertext()).strip()

    for k,v in record.items():
        if k.endswith('_list'):
            if not record[k]:
                record[k] = []

    # Flatten agency fields
    for p in ['','parent_']:
        if p+'agency' in record:
            for line in record[p+'agency']:
                for k,v in line.items():
                    record[p+'agency_'+k] = v
            del record[p+'agency']

    # Simplify lists where each item is single-item dict
    for c in ['govt_level','cfr','unfunded_mandate','legal_authority','small_entity']:
        k = c+'_list'
        # k = c[:-1]+'ies' if c.endswith('y') else c+'s'
        if k in record:
            if not record[k]:
                record[k] = []
            else:
                record[k] = [d[c] for d in record.get(k,[]) if (d[c] and d[c].lower() not in ['no','na','none'])]

    if 'legal_dline_list' in record:
        record['legal_deadline_list'] = [{k.replace('dline_',''):v for k,v in d.items()} for d in record['legal_dline_list']]
        del record['legal_dline_list']

    if 'timetable_list' in record:
        record['timetable'] = [{k.replace('ttbl_',''):v for k,v in d.items()} for d in record['timetable_list']]
        del record['timetable_list']

        # for event in record
        # if 'date' in record:
        #     date_match = re.match(r'(\d\d)/(\d\d)/(\d\d\d\d)',record['date']):
        #

        if citation_finder:
            # Identify frdoc numbers for timetable documents
            linked_timetable = []
            for event in record['timetable']:
                event = event.copy()
                if 'fr_citation' in event:
                    try:
                        finder_results = citation_finder(event['fr_citation'])

                    except ValueError:
                        print(f'Warning: Could not parse fr_citation "{event["fr_citation"]}"')
                        continue

                    if len(finder_results) == 1:
                        event['frdoc_number'] = finder_results[0]['frdoc_number']
                        event['citation_resolved_by'] = 'unique_match'

                    elif len(finder_results) > 1:
                        candidates_df = load_info_df([result['frdoc_number'] for result in finder_results],
                                                        fields=['frdoc_number','regulation_id_numbers','cfr_references','agencies','agencies_short','title'])

                        # Try refining by RIN first
                        candidates_df['rin_match'] = [(record['rin'] in rins) for rins in candidates_df['regulation_id_numbers']]
                        df = candidates_df[candidates_df['rin_match']]
                        if len(df) == 1:
                            event['frdoc_number'] = df['frdoc_number'].values[0]
                            event['citation_resolved_by'] = 'rin'
                            continue

                        # Refine by agency
                        rin_agencies = {record[k] for k in ['agency_name','parent_agency_name','agency_acronym','parent_agency_acronym'] if k in record}
                        if rin_agencies:
                            candidates_df['agency_name_match'] = [bool(rin_agencies & set(agencies)) for agencies in candidates_df['agencies']]
                            candidates_df['agency_acronym_match'] = [bool(rin_agencies & set(agencies)) for agencies in candidates_df['agencies_short']]
                            candidates_df = candidates_df[candidates_df['agency_name_match'] | candidates_df['agency_acronym_match']].copy()
                            if len(candidates_df) == 1:
                                event['frdoc_number'] = candidates_df['frdoc_number'].values[0]
                                event['citation_resolved_by'] = 'agency'
                                continue

                            # # Try refining by CFR
                            # if 'cfr_list' in record:
                            #     rin_cfrs = set(record['cfr_list'])
                            #     candidates_df['cfr_match'] = [bool(rin_cfrs & set(cfr_refs)) for cfr_refs in candidates_df['cfr_references']]
                            #     df = candidates_df[candidates_df['cfr_match']]
                            #     if len(df) == 1:
                            #         event['frdoc_number'] = df['frdoc_number'].values[0]
                            #         event['citation_resolved_by'] = 'cfr'
                            #         continue

                            # Try refining by keyword
                            search_text = ' '.join([record[field] for field in ['rule_title','abstract'] if record[field]])
                            finder_results = citation_finder(event['fr_citation'],keywords=search_text)

                            if (finder_results[0]['keyword_jaccard'] > 0.01) and (finder_results[0]['keyword_jaccard'] > finder_results[1]['keyword_jaccard']):
                                event['frdoc_number'] = finder_results[0]['frdoc_number']
                                event['citation_resolved_by'] = 'keyword_similarity'

                    # if 'frdoc_number' not in event:
                    #     print(f"Warning: Could not resolve citation \"{event['fr_citation']}\" for {record['publication']}/{record['rin']}")

                linked_timetable.append(event)

            record['timetable'] = linked_timetable

    return record


def iter_raw_agenda_records(agenda_dir):

    agenda_files = [agenda_dir/f for f in os.listdir(agenda_dir) if f.lower().endswith('.xml')]

    for agenda_file in tqdm(agenda_files):
        with open(agenda_file,'rb') as f:
            parser = et.XMLParser(recover=True)
            tree = et.parse(f,parser)
            for rin_element in tree.xpath('.//RIN_INFO'):
                record = element_to_dict(rin_element)
                yield record


def main(args=None):
    print('Compiling cleaned agenda files')

    raw_agenda_dir = Path(data_dir) / 'raw' / 'agenda'
    clean_agenda_dir = Path(data_dir) / 'agenda'

    if not os.path.isdir(clean_agenda_dir):
        os.mkdir(clean_agenda_dir)

    citation_finder = CitationFinder(enable_keywords=True)

    n_cleaned = 0
    frdocs_found = []
    frdocs_missed = []
    for record in iter_raw_agenda_records(raw_agenda_dir):

        clean_record = clean_agenda(record,citation_finder)

        if not os.path.isdir(clean_agenda_dir / clean_record['publication']):
            os.mkdir(clean_agenda_dir / clean_record['publication'])

        with open(clean_agenda_dir / clean_record['publication'] / f"{clean_record['rin']}.msgpack",'wb') as f:
            dump(clean_record,f)

        n_cleaned += 1
        if 'timetable' in clean_record:

            frdocs_found.extend([(clean_record['publication'],clean_record['rin'],event['frdoc_number'])
                                    for event in clean_record['timetable'] if ('frdoc_number' in event)])
            frdocs_missed.extend([(clean_record['publication'],clean_record['rin'],event['fr_citation'])
                                    for event in clean_record['timetable'] if ('fr_citation' in event) and ('frdoc_number' not in event)])

    print(f'Found and cleaned {n_cleaned} agenda records')
    success_rate = len(frdocs_found)/(len(frdocs_found) + len(frdocs_missed))
    print(f'Found FRDOC numbers for {len(frdocs_found)} timetable citations ({100*success_rate:.1f}% of success rate)')
    print(f'Failed to resolve {len(frdocs_missed)} citations. Examples include:')
    print('Examples include:\n\t' + '\n\t'.join([' / '.join(x) for x in random.sample(frdocs_missed,k=min(20,len(frdocs_missed)))]))


if __name__ == "__main__":
    main()


for record in iter_raw_agenda_records(raw_agenda_dir):
    if record['publication'][0]['publication_id'] == '200504':
        if record['rin'] == '2127-AH73':
            break


raw_record = record.copy()

event = record['timetable'][1]

record = raw_record.copy()
