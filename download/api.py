import requests
import time

baseURL = 'https://www.federalregister.gov/api/v1'
searchURL = baseURL + '/documents.json?'
articleURL = baseURL + '/documents/'

allFields = ['abstract', 'action', 'agencies',
             'agency_names', 'body_html_url', 'cfr_references',
             'citation', 'comments_close_on', 'correction_of',
             'corrections', 'dates', 'docket_id', 'docket_ids',
             'document_number', 'effective_on', 'end_page',
             'excerpts', 'executive_order_notes',
             'executive_order_number', 'full_text_xml_url',
             'html_url', 'images', 'json_url', 'mods_url', 'page_length',
             'page_views', 'pdf_url', 'president',
             'presidential_document_number', 'proclamation_number',
             'public_inspection_pdf_url', 'publication_date', 'raw_text_url',
             'regulation_id_number_info', 'regulation_id_numbers',
             'regulations_dot_gov_info', 'regulations_dot_gov_url',
             'significant', 'signing_date', 'start_page', 'subtype',
             'title', 'toc_doc', 'toc_subject', 'topics', 'type', 'volume']


def search(searchParams):
    response = get_with_retry(searchURL, searchParams,
                              retry_intervals=[1, 10, 100], timeout=100)
    results = response.json()

    if results['count'] > 10000:
        print('Warning: Results will be truncated at 10,000 entries')

    if results['count'] > 0:
        for result in results['results']:
            yield result

        while 'next_page_url' in results:
            nextPage = get_with_retry(results['next_page_url'],
                                      retry_intervals=[1, 10, 100], timeout=100)
            results = nextPage.json()

            for result in results['results']:
                yield result


def get_all_agencies(retry_intervals=[1, 10]):
    response = get_with_retry(baseURL + '/agencies',
                              retry_intervals=retry_intervals)
    return response.json()





def get_with_retry(url, params={}, retry_intervals=[1, 10, 60],
                   wait_interval=60, timeout=10, check_json=False):
    '''
    A wrapper for requests.get that tries to handle most of the common reasons
    to retry including:
        -Miscellaneous timeouts and connection errors other than bad-request
        -Rate limits (via wait_interval)
        -corrupted json (optional)
    '''

    if retry_intervals:
        for retry_interval in retry_intervals:
            try:
                r = requests.get(url, params=params, timeout=timeout)
            except requests.exceptions.ConnectionError:
                time.sleep(retry_interval)
                continue
            if r.ok:
                if check_json:
                    # Optional: retry if json content is corrupted
                    try:
                        r.json()
                    except Exception:
                        continue

                # Return successful request
                return r

            elif r.status_code == 429 and wait_interval > 0:
                # If rate-limit reached, wait and then use recursion to
                # restart retry intervals
                # (Never gives up unless a different error occurs)
                print('Rate limit reached. Waiting {:.1f} min to retry'
                      .format(wait_interval / 60.0))
                time.sleep(wait_interval)
                return get_with_retry(url, params=params,
                                      retry_intervals=retry_intervals,
                                      wait_interval=wait_interval,
                                      timeout=timeout,
                                      check_json=check_json)

            elif 400 <= r.status_code <= 500:
                # Don't bother to retry if the request is bad
                r.raise_for_status()

            else:
                # Retry if any other error occurs
                time.sleep(retry_interval)
        else:
            r = requests.get(url, params=params, timeout=timeout)

            # Still want to handle rate limits without other retries.
            if r.status_code == 429 and wait_interval > 0:
                # If rate-limit reached, wait and then use recursion to
                # restart retry intervals
                # (Never gives up unless a different error occurs)
                print('Rate limit reached. Waiting {:.1f} min to retry'
                      .format(wait_interval / 60.0))

                time.sleep(wait_interval)
                return get_with_retry(url, params=params,
                                      retry_intervals=retry_intervals,
                                      wait_interval=wait_interval,
                                      timeout=timeout,
                                      check_json=check_json)

            else:
                r.raise_for_status()
                return r


def get(url, params={}, timeout=10, ignore_codes=[], check_json=False):
    print('Warning: requestsUtilities.get is depreciated. Use get_with_retry.')
    return get_with_retry(url, params=params, retry_intervals=[],
                          timeout=timeout, check_json=check_json)
