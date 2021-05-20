import os
import lxml.etree as et
import pandas as pd
import numpy as np
import regex as re


def get_element_text(element):
    """
    Extract text while
    -skipping footnote numbers
    -Adding a space before and after emphasized text
    """
    head = element.text if element.tag != 'SU' else ''

    child = ' '.join(s for e in element.getchildren()
                     for s in [get_element_text(e)] if s)

    tail = element.tail

    return ' '.join(s for s in (head,child,tail) if s)


def update_header(header,element):
    """
    Track hierarchical headers
    """
    header_text = ''.join([s for s in element.itertext()])

    level_string = element.get('SOURCE','HED')
    new_level = int(level_string[-1].replace('D','0')) + 1

    if new_level < len(header):
        header = header[:new_level-1] + [header_text]
    else:
        header += [''] * (new_level - len(header))  # Make header length = new_level
        header[-1] = header_text  # Make last entry equal to header text
    return header


def get_ancestor(element,tag):
    for a in element.iterancestors():
        if a.tag == tag:
            return a
    return None


def parse_cfr_string(s,verbose=False):
    try:
        title = s.split()[0]
        for part in re.search(r'CFR(.*?(?P<part>(_|\d+|\b[IVXCL]+\b)))+',s,re.IGNORECASE).captures('part'):
            yield (title,part)
    except Exception:
        if verbose: print('Warning: Failed to parse CFR string "%s"' % s)
        yield (np.nan,np.nan)


def build_lstsub_cfr_map(root,verbose=False):
    # First associate each lstsub with the appropriate part elements
    lstsub_map = {e:[] for e in root.xpath('.//LSTSUB')}
    lstsub = None
    for e in root.xpath('.//LSTSUB|.//PART'):
        if e.tag == 'LSTSUB':
            lstsub = e
        elif lstsub is not None:
            lstsub_map[lstsub].append(e)

    # Then create a map from part elements to cfr
    cfr_map = {}
    for lstsub,part_elements in lstsub_map.items():

        cfrs = [cfr for e in lstsub.xpath('./CFR')
                for cfr in parse_cfr_string(get_element_text(e),verbose)]

        if len(cfrs) == len(part_elements):
            for cfr,part_element in zip(cfrs,part_elements):
                cfr_map[part_element] = cfr
    return cfr_map


def build_header_part_map(root):
    part_map = {}
    for part_element in root.xpath('.//PART'):
        for header_element in part_element.xpath(r'descendant::HD[1]'):
            s = get_element_text(header_element)
            for m in re.finditer(r'part\s+(\d+|_)',s,re.IGNORECASE):
                part_map[part_element] = m[1]
    return part_map


def build_part_cfr_map(root,verbose=False):
    """
    Build a mapping from part elements to CFR (title,part) tuples.
    If document has one CFR reference an one part element then mapping is trivial.
    If document has multiple CFR references and/or multiple part elements, *try*
    to infer correct CFR for each part element using the "List of Subjects"
    table that comes before a CFR reg part, and part headers.
    """

    doc_cfrs = [cfr for e in root.xpath('.//PREAMB/CFR')
                for cfr in parse_cfr_string(get_element_text(e),verbose)]

    part_elements = root.xpath('.//PART')

    if not doc_cfrs and not part_elements:
        return {}

    elif len(doc_cfrs) == 1 and len(part_elements) == 1:
        # Trivial case with one CFR part and no ambiguity
        return {part_elements[0]:doc_cfrs[0]}
    else:
        # Multiple sections case. Use lstsub and header information to infer CFR.
        lstsub_cfr_map = build_lstsub_cfr_map(root)
        header_part_map = build_header_part_map(root)

        cfr_map = {}
        for e in part_elements:
            if e in lstsub_cfr_map:
                cfr_map[e] = lstsub_cfr_map[e]
                if e in header_part_map and lstsub_cfr_map[e][1] != header_part_map[e]:
                    if verbose: print('Warning: lstsub and header cfr do not agree for %s\nlstsub_cfr_map=%s\nheader_part_map=%s' % (str(e),str(lstsub_cfr_map),str(header_part_map)))
            elif e in header_part_map:
                part = header_part_map[e]
                potential_titles = set(cfr[0] for cfr in doc_cfrs if cfr[1] == part)
                if part == '_':
                    title = '_'
                elif len(potential_titles) == 1:
                    title = list(potential_titles)[0]
                else:
                    title = np.nan
                    if verbose: print('Warning: Could not infer title for part element %s\npotential_titles=%s' % (str(e),str(potential_titles)))
                cfr_map[e] = (title,part)
            else:
                cfr_map[e] = (np.nan,np.nan)
                if verbose: print('Warning: Could not infer CFR for part element %s' % str(e))

        return cfr_map


def split_numbering(s):
    if type(s) is str:
        bracketed = re.match(r'\s*?((\(\s*(\d+|[A-Z]{1,3}|[a-z]{1,3})\s*\))\s*)+',s)
        if bracketed:
            number = bracketed.group(0)  # re.sub('\s+','',bracketed.group(0))
            s = s[len(bracketed.group(0)):]
            return number,s

        dotted = re.match(r'\s*((\d+|[A-Z]+)\.)\s',s)
        if dotted:
            number = dotted.group(0)  # .strip()
            s = s[len(dotted.group(0)):]
            return number,s

    return np.nan,s


def clean_paragraph_text(s):
    '''
    Adjust paragraph text, primarily to improve spacy tokenization
    (kind of a hack, but oh well)
    '''
    #Add spaces to split on opening brackets
    s = re.sub(r'(\S)\(',r'\g<1> (',s)

    #Add spaces around emdash and dash
    s = re.sub(r'(\S)([\u2014-])(\S)',r'\g<1> \g<2> \g<3>',s)

    return s


def parse_xml_file(xmlFile,**args):
    with open(xmlFile,'rb') as f:
        tree = et.parse(f)

    return parse_reg_xml_tree(tree,**args)


def parse_reg_xml_tree(tree,nlp=None,extract_numbering=True,verbose=False,
                       split_paragraphs=True):

    root = tree.getroot()

    part_cfr_map = build_part_cfr_map(root,verbose)

    paragraphs = []
    header = []
    part_element = None  # Need to track part elements because they are not hierarchical
    for element in root.iter():
        if element.tag == 'PART':
            part_element = element

        elif element.tag == 'HD':
            header = update_header(header,element)

        if element.tag in ['P','AMDPAR','HD','EXTRACT','GPOTABLE','SECTION']:
            text = get_element_text(element)

            paragraph = {
                        'xml_path':tree.getpath(element),
                        'header':tuple(header),
                        'legal':False,
                        'tag':element.tag
                        }

            reg_element = get_ancestor(element,'REGTEXT')
            if reg_element is not None:
                paragraph['legal'] = True
                paragraph['cfr_title'] = reg_element.get('TITLE')
                paragraph['cfr_part'] = reg_element.get('PART')

            elif part_element is not None and re.search(r'(SECTION|AMDPAR|EXTRACT|SUBPART)',paragraph['xml_path']):
                paragraph['legal'] = True
                paragraph['cfr_title'],paragraph['cfr_part'] = part_cfr_map[part_element]

            section_element = get_ancestor(element,'SECTION')
            if section_element is not None:
                try:
                    paragraph['cfr_section'] = section_element.xpath('./SECTNO/text()')[0].split()[1]
                except Exception:
                    if verbose: print('Warning: Failed to get section number for section %s' % section_element)
                try:
                    paragraph['section_subject'] = section_element.xpath('.//SUBJECT/text()')[0]
                except Exception:
                    if verbose: print('Warning: Section %s has no subject information' % section_element)

            ftnt_element = get_ancestor(element,'FTNT')
            if ftnt_element is not None:
                try:
                    paragraph['footnote'] = ftnt_element.xpath('.//SU/text()')[0]
                except Exception:
                    paragraph['footnote'] = 0
                    if verbose: print('Warning: Footnote %s has no numbering information' % ftnt_element)
            else:
                paragraph['footnotes'] = element.xpath('./SU/text()')

            """
            Agencies are inconsistent about how they use paragraph formatting:
            -In some documents, XML <p> items correspond to paragraphs
            -In some documents, <p> items contain multiple paragraphs split by newline characters

            For the sake of consistentency, split all paragraphs on newlines by default,
            keeping trailing whitespace with each paragraph
            """
            if split_paragraphs:
                par_texts = [m.group(0) for m in re.finditer(r'\s*.*\n?\s*',text)]

            else:
                par_texts = [text]

            for par_text in par_texts:

                if extract_numbering:
                    paragraph['numbering'],par_text = split_numbering(par_text)

                paragraph['text'] = par_text
                paragraphs.append(paragraph.copy())

    paragraph_df = pd.DataFrame(paragraphs)

    # Ensure dataframe has all columns
    for c in ['cfr_title','cfr_part','section','section_subject','footnote','footnotes']:
        if c not in paragraph_df.columns:
            paragraph_df[c] = np.nan

    # paragraph_df['text'] = paragraph_df['text'].apply(clean_paragraph_text)

    if nlp is not None:
        paragraph_df['doc'] = paragraph_df['text'].apply(nlp)

    return paragraph_df


def clean_html_text(text):
    # Strip header and footer
    text = re.sub(r'.+(?=AGENCY)','',text,flags=re.DOTALL)
    text = re.sub(r'\[(FR )?Doc.+?$','',text,flags=re.DOTALL | re.REVERSE)

    # Replace bullets with bullet char
    text = re.sub(r'<bullet>','•',text)
    # Replace doube-dash with em-dash
    text = re.sub(r'(?<=[^\s-])--(?=[^\s-])','—',text)
    # Replace 'Sec.' with §
    text = re.sub(r'(?<=\s)Sec\.','§',text)

    # Remove html tags (not worth parsing)
    text = re.sub(r'<\\?.+?>','',text)

    # #Remove dashed horizontal lines
    # text = re.sub(r'\n-{5,}\n','\n',text)

    # Delete page markers
    text = re.sub(r'\n\s*\[\[Page.+\]\]\s*\n',' ',text,flags=re.IGNORECASE)

    # Replace in-paragraph line-breaks with spaces
    text = re.sub(r'[ -]\n(?=\S)',' ',text)

    # Convert inline titles to their own lines (starting next line with tab)
    text = re.sub(r'(?<=(^|\n)[^a-z]+:\s)','\n    ',text)

    return text


def tag_html_paragraph(s):
    if s.startswith('<P>'):
        return 'P'
    elif re.match(r'\d\n\S',s):
        return 'AMDPAR'
    elif re.match(r'§\s+\d',s):
        return 'SECTION'
    elif re.match(r'\*+\s*',s):
        return 'STARS'
    elif re.match(r'\S',s):
        if '\n' in s.strip():
            return 'EXTRACT'
        else:
            return 'HD'


def parse_footnotes(s):
    footnote_pattern = r'(?<!\d\s*)\\(?P<i>\d+)\\'
    # Parse footnotes
    footnote_paragraph_match = re.match(footnote_pattern,s)
    if footnote_paragraph_match:
        footnote = footnote_paragraph_match.group('i')
        footnotes = []
    else:
        footnote = np.nan
        footnotes = [m.group('i') for m in re.finditer(footnote_pattern,s)]

    s = re.sub(footnote_pattern,'',s)

    return footnote,footnotes,s


def line_code(line):
    # line = line.strip()
    if not line.strip():
        return ' '
    if line.count('-') == len(line):
        return '-'

    if line.startswith('     '):
        return 'c'

    stars = line.count('*')
    if stars >= 0.5*len(line):
        return '*'

    alphas = sum(map(str.isalpha,line))
    if alphas + stars > 0.5*len(line):
        return 'a'

    return '.'

# ''.join(map(line_code,text.split('\n')))

# print('\n'.join(map(lambda line: line_code(line)+'   '+line,text.split('\n'))))


def split_tables(text):

    '''
    Identify tables using pseudo-likelyhood approach.
    Want to create table partitions (start line, end line) such that:
        -Tables are at least 3 lines long
        -Tables start and end with a horizontal line
        -Tables sort common table chars {'-',' ','.'} from other chars
        as completely as possible
    '''

    lines = text.split('\n')
    line_codes = ''.join(map(line_code,lines))

    table_matches = list(re.finditer(r'.(-[ c\.]*){2,}',line_codes))
    if table_matches:
        for table_match in table_matches:

            pre_table = '\n'.join(lines[:table_match.start()])
            table = '\n'.join(lines[table_match.start():table_match.end()])

            yield pre_table,table

        if table_match.end() < len(lines):
            yield '\n'.join(lines[table_match.end():]),''
    else:
        yield text,''


def extract_html_paragraphs(text,extract_numbering=True):

    header = None
    legal_header = False
    legal = False
    # Split tables
    for text,table in split_tables(text):

        # Mark paragraphs indicated by leading tabs (4 spaces)
        text = re.sub(r'(?<=\n) {4}(?!\s)','    <P>',text)

        for s in (m.group(0) for m in re.finditer(r'.+?[^\n]($|\n{2}\s*|(?=<P>))',text,flags=re.DOTALL)):
            # Skip dashed lines
            if not re.match(r'^-{5,}\s*$',s):
                tag = tag_html_paragraph(s)
                footnote = np.nan
                footnotes = []

                # Drop dashed lines
                s = re.sub(r'\n-{5,}\n','\n',s)

                if s:
                    if tag == 'P':
                        # Trim tab indentation
                        s = s[3:]

                        # Parse and remove footnote numbers
                        footnote,footnotes,s = parse_footnotes(s)

                    elif tag == 'AMDPAR':
                        # Trim amendment indicator ("0")
                        s = s[2:]

                    elif tag == 'HD':
                        header = s.strip()
                        legal_header = bool(re.match(r'(Part\s\d+|Subpart|§|Appendix)',header,re.IGNORECASE))

                    elif tag == 'SECTION':
                        header = s.strip()
                        legal_header = True

                    legal = legal_header or tag in {'AMDPAR','SECTION','STARS'}

                    paragraph_info = {'tag':tag,'text':s,'footnote':footnote,'footnotes':footnotes,'header':header,'legal':legal}

                    if extract_numbering:
                        paragraph_info['numbering'],paragraph_info['text'] = split_numbering(s)

                    yield paragraph_info

        if table:
            yield {'tag':'GPOTABLE','text':table,'header':header,'legal':legal}


def parse_html(s,extract_numbering=True):
    text = clean_html_text(s)
    parsed_df = pd.DataFrame(list(extract_html_paragraphs(text,extract_numbering=extract_numbering)))

    return parsed_df


def parse_html_file(html_filename,extract_numbering=True):
    with open(html_filename,encoding='utf8') as f:
        text = f.read()
    return parse_html(text,extract_numbering=extract_numbering)


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
