import os
from argparse import ArgumentParser
from pathlib import Path
import random
from collections import Counter
from tqdm import tqdm
import gzip
from lxml import etree as et
import pandas as pd

from frdocs.preprocessing.parsing import parse_reg_xml_tree, FrdocResolver
from frdocs.config import data_dir

'''
This script compiles parsed versions of each document from bulk XML files.
Each parsed document is a pandas dataframe, saved in pickle format. Files are
named by document number so that they can be retreived without looking up the
publication date.
'''


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
