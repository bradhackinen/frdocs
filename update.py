from argparse import ArgumentParser
from pathlib import Path
from datetime import date,datetime

from frdocs.download.download_meta import main as meta_main
from frdocs.download.download_xml import main as xml_main
from frdocs.parse.compile_parsed import main as parse_main

from frdocs.config import data_dir

if __name__ == "__main__":

    parser = ArgumentParser()

    parser.add_argument('--start_date',type=str,default=str(date(2000,1,1)))
    parser.add_argument('--end_date',type=str,default=str(datetime.today().date()))
    parser.add_argument('--force_update',dest='force_update',action='store_true')

    parser.add_argument('--meta_dir',type=str,default=str(Path(data_dir) / 'meta'))
    parser.add_argument('--xml_dir',type=str,default=str(Path(data_dir) / 'xml'))
    parser.add_argument('--parsed_dir',type=str,default=str(Path(data_dir) / 'parsed'))

    parser.add_argument('--skip_meta',dest='skip_meta',action='store_true')
    parser.add_argument('--skip_xml',dest='skip_xml',action='store_true')
    parser.add_argument('--skip_parsed',dest='skip_parsed',action='store_true')

    args = parser.parse_args()

    if not args.skip_meta:
        meta_main(args)
    if not args.skip_xml:
        xml_main(args)
    if not args.skip_parsed:
        parse_main(args)
