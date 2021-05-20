# FRDocs
*An interface for collecting and parsing Federal Register documents*

## Installation
Clone the code from github.

The module requires a file named `config.py` in the root project directory. The config file must define a variable named `data_dir` which points to the root directory where the data will be saved.

## Data collection
The module collects data from two sources:
1. `data_collection/download_meta.py` downloads ***metadata*** describing Federal Register documents from the [federalregister.gov](https://www.federalregister.gov/) API. Raw metadata is saved in annual zipped json files in `data_dir/meta`.

2. `data_collection/download_xml.py` downloads the ***text*** of daily Federal Register documents from the [govinfo.gov](https://www.govinfo.gov/). Raw XML files are saved in `data_dir/xml`.

`dataCollection/compile_parsed.py` builds ***parsed*** versions of the documents, where the XML is converted into Pandas data tables. These files are saved as pickled dataframes in `data_dir/parsed`. Files are named by document number, which must be extracted from the XML itself (and occasionally contains errors). The XML files sometimes contain duplicate printings of the same document, but each document only appears once in the parsed directory.

**The complete dataset can be downloaded from scratch or updated to the latest available data by running `update.py`.**

The complete dataset is approximately 20GB in size.

## Loading data
- `frdocs.load_info_df()` loads all document metadata as a single dataframe
- `frdocs.iter_parsed()` iteratively loads available parsed documents
- `frdocs.load_parsed()` loads a single parsed document
