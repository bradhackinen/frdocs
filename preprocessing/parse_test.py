import os
from pathlib import Path

from frdocs.preprocessing.parsing import parse_html_file,parse_xml_file
from frdocs import project_dir


test_dir = project_dir / 'preprocessing' / 'test_docs'

for f in sorted(os.listdir(test_dir)):
    print(f'Parsing {f}')

    if Path(f).suffix == '.xml':

        parsed_df = parse_xml_file(test_dir/f)
        print(parsed_df)

        print('Verfying paragraph splitting',end='')
        assert ''.join(parse_xml_file(test_dir/f, split_paragraphs=True,extract_numbering=False)['text']) \
            == ''.join(parse_xml_file(test_dir/f, split_paragraphs=False,extract_numbering=False)['text'])
        print(' (Successful)')

    elif Path(f).suffix.startswith('.htm'):
        continue
        parsed_df = parse_html_file(test_dir/f)
        print(parsed_df)

    else:
        print(f'Unkown frdoc file type {Path(f).suffix}')
