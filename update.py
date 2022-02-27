from argparse import ArgumentParser
from pathlib import Path
from datetime import datetime

from frdocs.download import download_agenda, download_meta, download_xml
from frdocs.preprocessing import compile_agenda, compile_info, compile_parsed

from frdocs.config import data_dir

if __name__ == "__main__":

    parser = ArgumentParser()

    parser.add_argument('--start_date',type=str,default='1994-01-03')
    parser.add_argument('--end_date',type=str,default=str(datetime.today().date()))
    parser.add_argument('--force_update',dest='force_update',action='store_true')

    parser.add_argument('--agenda_dir',type=str,default=str(Path(data_dir) / 'agenda'))
    parser.add_argument('--meta_dir',type=str,default=str(Path(data_dir) / 'meta'))
    parser.add_argument('--xml_dir',type=str,default=str(Path(data_dir) / 'xml'))
    parser.add_argument('--parsed_dir',type=str,default=str(Path(data_dir) / 'parsed'))

    parser.add_argument('--skip_info',dest='skip_info',action='store_true')
    parser.add_argument('--skip_xml',dest='skip_info',action='store_true')
    parser.add_argument('--skip_agenda',dest='skip_agenda',action='store_true')
    parser.add_argument('--skip_parsed',dest='skip_parsed',action='store_true')

    parser.add_argument('--skip_download',dest='skip_download',action='store_true')
    parser.add_argument('--skip_preprocessing',dest='skip_preprocessing',action='store_true')

    args = parser.parse_args()

    # Download raw data
    if not args.skip_download:
        if not args.skip_info:
            download_meta.main(args)
        if not args.skip_agenda:
            download_agenda.main(args)
        if not (args.skip_parsed or args.skip_xml):
            download_xml.main(args)

    # Preprocess data
    if not args.skip_preprocessing:
        if not args.skip_info:
            compile_info.main(args)
        if not args.skip_agenda:
            compile_agenda.main(args)
        if not args.skip_parsed:
            compile_parsed.main(args)
