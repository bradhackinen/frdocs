import os
from argparse import ArgumentParser
from pathlib import Path
import random
from collections import Counter
import re
from tqdm import tqdm
from unidecode import unidecode
import gzip
from lxml import etree as et
import pandas as pd

from frdocs.preprocessing.parsing import parse_reg_xml_tree
from frdocs import load_info_df

from frdocs.config import data_dir

'''
This script compiles parsed versions of each document from bulk XML files.
Each parsed document is a pandas dataframe, saved in pickle format. Files are
named by document number so that they can be retreived without looking up the
publication date.
'''


def extract_frdoc_number(s):
    '''
    This function extracts the document from an FRDOC string. The standard
    format is something like:
        "[FR Doc. 12-1149 Filed 1-18-12; 8:45 am]"
    Where "12-1149" is the document number. However, contents are clearly
    manually entered, because there can be a variety of deviations from this
    format. For example:
        "[FR Doc.12-1149; Filed 1-18-12; 8:45 am]"
        "[FR Doc. 12-1149 1-18-12; 8:45 am]"
        "[JR Doc. 12-1149 Filed 1-18-12; 8:45 am]"
        "[FR Doc. 12- 1149 Filed 1-18-12; 8:45 am]"
        "[FR Doc. 1149 Filed 1-18-12; 8:45 am]"

    Many document numbers also start with "E" or "Z" instead of the first digits
    of the year.

    Reprints and corrections are also sometimes labeled by prepending to the
    document number with something like "C1-", "R1-", or "X"

    This function assumes the document number is located either immediately
    following "FR Doc.", or immediately preceeding "Filed". Document numbers are
    allowed to start with E or Z (as in "E7-1592").

    Sometimes the year component is ommitted ("12-" in the example above).
    In these cases, the function attempts to locate the year from the date,
    assuming it appears just before the time, and prepends it to the document
    number.

    Finally, the document number is standardized by converting to ascii,
    removing all whitespace, and making letters uppercase.

    If the function cannot parse the document number it prints a warning and
    returns None.
    '''

    # Define the general fr doc pattern with up to three parts
    fr_pattern = r'''((?P<part1>[CRX]\d*)[\s-]*)?   # Optional prepended part
                     (?P<part2>[EZ]?\d+)            # Required initial or middle number
                     (\s?-+\s?(?P<part3>\d*))?       # Optional third number
                     '''
    s = unidecode(s)

    # Case 1: Format is "[FR Doc. #####..."
    m = re.search(fr'FR\sDoc\.?\s*{fr_pattern}',s,flags=re.IGNORECASE | re.VERBOSE)

    if not m or len(m.group(0)) < 3:
        # Case 2: Format is "...###### Filed..."
        m = re.search(fr'[.\s]{fr_pattern};?\s*Fil',s,flags=re.IGNORECASE | re.VERBOSE)

    if m:

        # Rebuild the document number from parts
        d = m.group('part2')
        if m.group('part1'):
            d = m.group('part1') + '-' + d
        if m.group('part3'):
            d = d + '-' + m.group('part3')

        d = unidecode(d)
        d = d.upper()

        return d

    else:
        print(f'Warning: Could not parse document number in "{s}"')
        return None


def standardize_frdoc_number(d):
    """
    The document numbers used in on federalregister.gov are also parsed from
    raw data, and can include errors such as small pieces of text appended to
    the end of the string.

    Document numbers also sometimes have multiple reprentations. For example,
        2005-0034
        05-0034
        5-0034
        E5-0034
        2005-34
    Would presumably all refer to the same document.

    This function standardizes documents numbers by:
        1) Removing any trailing non-numeric characters
        2) Dropping first two digits of the year when 4 digits are present
        3) Dropping leading zeros from the last number
    """
    try:
        # Remove trailing non-numeric chars
        d = re.sub(r'[^0-9]+$','',d)

        # Remove "E" - never seems completely necessary
        d = d.replace('E','')

        # Split into parts
        parts = d.rsplit('-')

        # Clean year. Could be in any part except the last.
        for i in range(len(parts)-1) in parts[:-1]:
            if re.match(r'(19|20?)\d\d',parts[i]):
                parts[i] = re.sub(r'(19|200?)','',parts[i])
                break

        try:
            parts[-1] = str(int(parts[-1]))
        except Exception:
            pass

        return '-'.join(parts)
    except Exception:
        return d


class FrdocResolver():
    def __init__(self):

        self.info_df = load_info_df(fields=['frdoc_number','publication_date','volume','start_page','end_page'])
        self.info_df['standardized_frdoc'] = self.info_df['frdoc_number'].apply(standardize_frdoc_number)

        self.all_frdocs = set(d for d in self.info_df['frdoc_number'].dropna() if d.strip())

    def __call__(self,doc_info):

        if doc_info['frdoc_string']:
            frdoc_number = extract_frdoc_number(doc_info['frdoc_string'])

            # Search based on extracted frdoc number
            if frdoc_number in self.all_frdocs:
                return frdoc_number

            # Search based on the standardized frdoc number
            standardized_frdoc = standardize_frdoc_number(frdoc_number)
            candidates_df = self.info_df[self.info_df['standardized_frdoc'] == standardized_frdoc]
            if len(candidates_df) == 1:
                return candidates_df['frdoc_number'].values[0]

        # Search based on the publication date, volume and pages (FR citation)
        candidates_df = self.info_df[(self.info_df['publication_date'] == doc_info['publication_date']) \
                                        & (self.info_df['volume'] == doc_info['volume']) \
                                        & (self.info_df['start_page'] == doc_info['start_page']) \
                                        & (self.info_df['end_page'] == doc_info['end_page'])]
        if len(candidates_df) == 1:
            return candidates_df['frdoc_number'].values[0]

        if doc_info['frdoc_string']:
            # Try to refine search by seeing if frdoc is within frdoc_string (need to strip whitespace)
            frdoc_string_nospace = re.sub(r'\s','',doc_info['frdoc_string'])
            candidates_df['frdoc_number_nospace'] = [re.sub(r'\s','',d) for d in candidates_df['frdoc_number']]
            candidates_df['frdoc_match'] = [(d in frdoc_string_nospace) for d in candidates_df['frdoc_number_nospace']]
            candidates_df = candidates_df[candidates_df['frdoc_match']]

            if len(candidates_df) == 1:
                return candidates_df['frdoc_number'].values[0]

        print('Warning: Could not resolve frdoc for document with the following identify info:')
        print(doc_info)
        if len(candidates_df):
            print('Candidates:')
            print(candidates_df)
        else:
            print('Candidates: None')

        return None


def iter_docs(xml_dir):
    for xml_file in tqdm(sorted(os.listdir(xml_dir))):

        pub_date = xml_file.split('.')[0]

        with gzip.open(xml_dir / xml_file,'rb') as f:
            tree = et.parse(f)

            volume = int(tree.xpath('.//VOL/text()')[0])

            for fr_type in ['NOTICE','PRORULE','RULE']:
                for type_element in tree.xpath(f'.//{fr_type}S'):

                    try:
                        start_page = int(type_element.xpath('.//PRTPAGE/@P')[0])
                    except IndexError:
                        start_page = -1

                    for doc_element in type_element.xpath(f'.//{fr_type}'):
                        # doc_tree = et.ElementTree(doc_element)

                        doc = {
                                'doc_tree':et.ElementTree(doc_element),
                                # 'fr_type':fr_type.lower(),
                                'volume':volume,
                                'publication_date':pub_date,
                                'start_page':start_page,
                                }

                        # Get end page from page elements
                        print_pages = [int(page) for page in doc_element.xpath('.//PRTPAGE/@P') if page.isdigit()]
                        doc['end_page'] = max([start_page] + print_pages)

                        # End page for this doc is start page for next doc
                        start_page = doc['end_page']

                        # Can only get the FR document number from the end of the document
                        frdoc_elements = doc_element.xpath('./FRDOC')

                        if not frdoc_elements:
                            print(f'Warning: Could not find FRDOC element in {xml_file}: {tree.getpath(doc_element)}')
                            doc['frdoc_string'] = None

                        elif len(frdoc_elements) > 1:
                            print(f'Warning: Found {len(frdoc_elements)} FRDOC elements in {xml_file}: {tree.getpath(doc_element)}')
                            doc['frdoc_string'] = None

                        else:
                            doc['frdoc_string'] = ' '.join(frdoc_elements[0].itertext())

                        yield doc


def main(args):
    print('Parsing documents from daily XML files')

    xml_dir = Path(data_dir) / 'raw' / 'xml'
    parsed_dir = Path(data_dir) / 'parsed'

    if not os.path.isdir(parsed_dir):
        os.mkdir(parsed_dir)

    frdoc_resolver = FrdocResolver()

    if not args.force_update:
        existing = {f.rsplit('.',1)[0] for f in os.listdir(parsed_dir)}
        print(f'Found {len(existing)} existing parsed files ({len(frdoc_resolver.all_frdocs - existing)} remaining to parse)')
    else:
        existing = set()

    n_parsed = 0
    frdoc_counts = Counter()
    failed = []
    for doc in iter_docs(xml_dir):
        frdoc = frdoc_resolver(doc)

        if frdoc:
            frdoc_counts.update([frdoc])

            if (frdoc not in existing) or args.force_update:

                parsed_df = parse_reg_xml_tree(doc['doc_tree'])
                parsed_df.to_pickle(parsed_dir/f'{frdoc}.pkl')

                existing.add(frdoc)
                n_parsed += 1
        else:
            failed.append(doc)

    print(f'Parsed {n_parsed} new documents')
    completeness = len(existing)/len(frdoc_resolver.all_frdocs)
    print(f'Database now has parsed documents, covering {100*completeness:.1f}% of frdoc numbers with metadata')

    missing = list(frdoc_resolver.all_frdocs - existing)
    if missing:
        print(f'Missing parsed documents for {len(missing)} frdoc numbers ')
        print('Examples include:\n\t' + '\n\t'.join(random.sample(missing,k=min(20,len(missing)))))

    n_dups = sum(c > 1 for c in frdoc_counts.values())
    print(f'{n_dups} resolved document numbers appear multiple times')
    if n_dups:
        common_dups = {d:c for d,c in frdoc_counts.most_common(20) if c > 1}
        print('Most common examples:\n\t' + '\n\t'.join(f'{d} (x{c})' for d,c in common_dups.items()))

    print(f'Failed to resolve frdoc numbers for {len(failed)} documents')
    if failed:
        print('Examples include:')
        for failed_doc in random.sample(failed,k=min(20,len(failed))):
            print(failed_doc)

    # Add parsed information to index
    print('Adding parsing success info to index')
    index_df = pd.read_csv(Path(data_dir)/'index.csv')
    index_df['parsed'] = index_df['frdoc_number'].isin(existing)
    index_df.to_csv(Path(data_dir)/'index.csv',index=False)

    if completeness < 1:
        missing_df = index_df[~index_df['parsed']]
        print('Missing parsed docs by top publication date (top 20):')
        print(missing_df.groupby('publication_date')[['frdoc_number']].count().sort_values('frdoc_number',ascending=False).head(20))


if __name__ == "__main__":

    parser = ArgumentParser()

    parser.add_argument('--force_update',dest='force_update',action='store_true')

    args = parser.parse_args()

    main(args)
