import os
from argparse import ArgumentParser
from pathlib import Path
from datetime import date, datetime, timedelta
from tqdm import tqdm
from msgpack import dump
from dateutil.parser import parse as parse_date

from frdocs.download import api
from frdocs.config import data_dir


def date_range(start, end):
    span = end - start
    for i in range(span.days):
        yield start + timedelta(days=i)


def main(args):
    meta_dir = Path(args.meta_dir)
    start_date = parse_date(args.start_date).date()
    end_date = parse_date(args.end_date).date()

    print('\nDownloading agency metdata')
    agencies = api.get_all_agencies()

    with open(Path(data_dir) / 'agencies.msgpack','wb') as f:
        dump(agencies,f)

    print(f'Downloading document metadata for {start_date} to {end_date}')

    if not os.path.exists(meta_dir):
        os.makedirs(meta_dir)

    # Search for existing files
    if args.force_update:
        remaining = list(date_range(start_date,end_date))
    else:
        existing = {f.split('.',1)[0] for f in os.listdir(meta_dir)}
        print(f'Found {len(existing)} existing daily files')

        remaining = [d for d in date_range(start_date,end_date)
                     if str(d) not in existing]
        print(f'Up to {len(remaining)} remain to download')

    invalid = 0
    downloaded = 0
    for d in tqdm(remaining):

        save_file = meta_dir / f'{d}.msgpack'

        searchParams = {
                        'order':'oldest',
                        'per_page':1000,
                        'fields[]':api.allFields,
                        'conditions[publication_date][is]':str(d),
                       }

        results = list(api.search(searchParams))

        if results:
            with open(save_file,'wb') as f:
                dump(results,f)

            downloaded += 1

        else:
            invalid += 1

    print(f'\nDownloaded {downloaded} daily search results')
    print(f'No results for {invalid} dates (these are usually weekends and holidays)')


if __name__ == "__main__":

    parser = ArgumentParser()

    parser.add_argument('--meta_dir',type=str,default=str(Path(data_dir) / 'meta'))
    parser.add_argument('--start_date',type=str,default=str(date(2000,1,1)))
    parser.add_argument('--end_date',type=str,default=str(datetime.today().date()))
    parser.add_argument('--force_update',dest='force_update',action='store_true')

    args = parser.parse_args()

    main(args)
