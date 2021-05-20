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

from frdocs.parse.parsers import parse_reg_xml_tree, standardize_frdoc_number
from frdocs import load_info_df

from frdocs.config import data_dir

'''
This script compiles parsed versions of each document from bulk XML files.
Each parsed document is a pandas dataframe, saved in pickle format. Files are
named by document number so that they can be retreived without looking up the
publication date. For files that are published multiple times, only the first
version published is included.
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

        if not m:
            print(f'Warning: Could not parse document number in "{s}"')
            return None

    if m:

        # Rebuild the document number from parts
        d = m.group('part2')
        if m.group('part1'):
            d = m.group('part1') + '-' + d
        if m.group('part3'):
            d = d + '-' + m.group('part3')

        # d = m.group(1)
        d = unidecode(d)
        d = d.upper()
        # d = re.sub('[^A-Z0-9\-]','',d) # Shouldn't be necessary given the

        if '-' not in d:
            # No year component in document number.
            # Try searching for it, assuming it appears just before time using colon (e.g. 8:45 am)
            m = re.search(r'-\s*(\d{2});\s*\d+:',s,re.IGNORECASE)
            if m:
                yy = m.group(1)
                d = f'{yy}-{d}'
                print(f'Warning: No year component found in "{s}". Guessing document number is {d}')
                return d

            else:
                print(f'Warning: Could not parse document number in "{s}"')
                return None

        return d


# standardize_frdoc_number('2005-0034')
# standardize_frdoc_number('05-0034')
# standardize_frdoc_number('2005-34')
# standardize_frdoc_number('E05-0034')


def main(args):

    xml_dir = Path(args.xml_dir)
    parsed_dir = Path(args.parsed_dir)

    if not os.path.isdir(parsed_dir):
        os.mkdir(parsed_dir)

    info_df = load_info_df(fields=['frdoc_number','publication_date','full_text_xml_url'])

    meta_docs = set(info_df['frdoc_number'].dropna())

    standardized_to_frdoc = {standardize_frdoc_number(d):d for d in meta_docs}

    n_lost = len(meta_docs) - len(standardized_to_frdoc)
    if n_lost:
        print(f'Warning: {n_lost} document numbers have non-unique standardized form')

    print(f'Building parsed files for {len(standardized_to_frdoc)} documents with metadata')

    if not args.force_update:
        existing = {f.rsplit('.',1)[0] for f in os.listdir(parsed_dir)}
        print(f'Found {len(existing)} existing parsed files ({len(meta_docs - existing)} remaining to parse)')

    parsed = 0
    dups = Counter()
    not_in_meta = Counter()
    for xmlFile in tqdm(sorted(os.listdir(xml_dir))):
        with gzip.open(xml_dir / xmlFile,'rb') as f:
            tree = et.parse(f)

            for frType in ['NOTICE','PRORULE','RULE']:
                for docElement in tree.xpath(f'.//{frType}'):
                    docTree = et.ElementTree(docElement)

                    # Have to get the FR document number from the end of the document
                    frdoc_elements = docTree.xpath('./FRDOC')

                    if not frdoc_elements:
                        print(f'Warning: Could not find FRDOC element in {f}: {tree.getpath(docElement)}')
                        continue

                    elif len(frdoc_elements) > 1:
                        print(f'Warning: Found {len(frdoc_elements)} FRDOC elements in {f}: {tree.getpath(docElement)}')
                        continue

                    s = ' '.join(frdoc_elements[0].itertext())

                    extracted_frdoc = extract_frdoc_number(s)

                    # Find the frdoc that shares the same standardized form
                    standardized_frdoc = standardize_frdoc_number(extracted_frdoc)

                    if standardized_frdoc in standardized_to_frdoc:

                        frdoc = standardized_to_frdoc[standardized_frdoc]

                        if frdoc in existing:
                            dups[frdoc] += 1

                        if (frdoc not in existing) or args.force_update:

                            parsed_df = parse_reg_xml_tree(docTree)
                            parsed_df.to_pickle(parsed_dir/f'{frdoc}.pkl')

                            existing.add(frdoc)
                            parsed += 1

                    else:
                        not_in_meta[extracted_frdoc] += 1


    print(f'Parsed {parsed} documents')
    print(f'Now have parsed documents for {100*len(existing)/len(meta_docs):.1f}% of documents with metadata')
    if len(existing) < len(meta_docs):
        missing_with_xml = set(info_df[info_df['full_text_xml_url'].notnull()]['frdoc_number']) - existing
        print(f'\tOf these missing documents, {len(missing_with_xml)} have xml urls in the metadata')
        if missing_with_xml:
            print('\tExamples include:\n\t\t' + '\n\t\t'.join(random.sample(missing_with_xml,k=min(100,len(missing_with_xml)))))
    print(f'{len(dups)} parsed document numbers appeared multiple times')
    if len(dups):
        print('\tMost common examples:\n\t\t' +'\n\t\t'.join(f'{d} (x{c+1})' for d,c in dups.most_common(100)))

    print(f'{len(not_in_meta)} parsed document numbers not found in the metadata.')
    if len(not_in_meta):
        print('\tMost common examples:\n\t\t'+'\n\t\t'.join(f'{d} (x{c})' for d,c in not_in_meta.most_common(100)))


if __name__ == "__main__":

    parser = ArgumentParser()

    parser.add_argument('--xml_dir',type=str,default=str(Path(data_dir) / 'xml'))
    parser.add_argument('--parsed_dir',type=str,default=str(Path(data_dir) / 'parsed'))
    parser.add_argument('--force_update',dest='force_update',action='store_true')

    args = parser.parse_args()

    main(args)
