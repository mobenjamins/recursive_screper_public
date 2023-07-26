import os
import sys
import csv
from urllib.parse import urlparse
import itertools
import json
import difflib
import requests
from datetime import datetime
from PyPDF2 import PdfReader
from io import BytesIO
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from base_script import *
import hashlib

csv.field_size_limit(2147483647)

def normalize_and_hash(text):
    def clean_text(text):
        lines = text.split("\n")
        cleaned_lines = [" ".join(line.strip().split()) for line in lines]
        cleaned_text = "\n".join(cleaned_lines)
        return cleaned_text

    # Clean the text
    text = clean_text(text)

    # Normalize the text: convert to lowercase and remove punctuation
    text = text.lower().translate(str.maketrans('', '', string.punctuation))

    # Hash the text using sha256
    return hashlib.sha256(text.encode('utf-8')).hexdigest()
    
def read_data_from_sheet(spreadsheet_url, sheet_index):
    print(f'\n[***] Reading data from sheet number {sheet_index+1} ...')
    
    DIR = os.path.dirname(os.path.realpath(__file__))
    parent_dir = os.path.dirname(DIR)
    root_dir = os.path.dirname(parent_dir)
    sys.path.append(parent_dir)

    # Get the JSON credentials file for the service account
    with open(credentials_path, "r") as file:
        secrets = json.load(file)

    credentials = ServiceAccountCredentials.from_json_keyfile_dict(secrets, ["https://www.googleapis.com/auth/spreadsheets"])

    # Connect to the Google Sheets API using the credentials
    client = gspread.authorize(credentials)

    # Open the spreadsheet using the URL
    spreadsheet = client.open_by_url(spreadsheet_url)

    # Get the desired worksheet in the spreadsheet
    worksheet = spreadsheet.get_worksheet(sheet_index)

    # Now worksheet is defined, you can use it to extract values
    column_b_values = worksheet.col_values(2)[1:]  # Exclude the header
    column_d_values = worksheet.col_values(4)[1:]
    column_e_values = worksheet.col_values(5)[1:]
    column_f_values = worksheet.col_values(6)[1:]
    column_g_values = worksheet.col_values(7)[1:]

    
    # Create a list of dictionaries
    data_list = []
    for b, d, e, f, g in itertools.zip_longest(column_b_values, column_d_values, column_e_values, column_f_values, column_g_values, fillvalue=""):
        data_dict = {b: [d, e, f, g]}
        data_list.append(data_dict)
    
    cleaned_data_list = []
    
    for data_dict in data_list:
        cleaned_dict = {}
        for key, phrases in data_dict.items():
            cleaned_phrases = [phrase.replace('\n', ' ') for phrase in phrases]
            cleaned_dict[key] = cleaned_phrases
        cleaned_data_list.append(cleaned_dict)

    return cleaned_data_list

def check_changes(old_data, new_data, url):
    if url in old_data and old_data[url] != new_data:
        print(f'[X] CHANGED ==> {url}')
        
        EMAILS_TO_SEND_TO = ['amine@thecozm.com', 'benjamin.oghene@thecozm.com', 'manny@thecozm.com']
        #EMAILS_TO_SEND_TO = ['amine@thecozm.com']
        send_email(EMAILS_TO_SEND_TO, 'Website Change Alert', f'There has been a change detected on {url}.\n\nNew data:\n{new_data}')
        
        old_data[url] = new_data
    else:
        print(f'[-] SAME ==> {url}')
        
def update_check_dates(spreadsheet_url, sheet_index, rows, date_format="%Y-%m-%d"):
    # Get the date as a string in the provided format
    date_str = datetime.now().strftime(date_format)

    # Connect to the Google Sheets API
    DIR = os.path.dirname(os.path.realpath(__file__))
    parent_dir = os.path.dirname(DIR)
    root_dir = os.path.dirname(parent_dir)
    sys.path.append(parent_dir)

    with open(credentials_path, "r") as file:
        secrets = json.load(file)
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(secrets, ["https://www.googleapis.com/auth/spreadsheets"])
    client = gspread.authorize(credentials)

    # Open the spreadsheet and get the worksheet
    spreadsheet = client.open_by_url(spreadsheet_url)
    worksheet = spreadsheet.get_worksheet(sheet_index)

    # Find the first column labeled "Checked", or if not present, the first empty column
    headers = worksheet.row_values(1)  # Assuming first row is the headers
    try:
        column_index = headers.index("Checked") + 1  # Add one due to 1-indexing in gspread
    except ValueError:
        # "Checked" not found, find first empty column
        column_index = headers.index("") + 1 if "" in headers else len(headers) + 1

    # If a new column is created, update the header to "Checked"
    if column_index == len(headers) + 1:
        worksheet.update_cell(1, column_index, "Checked")  # Update the header

    # Update the cells in the chosen column for the given rows
    cell_list = [gspread.Cell(row=row + 2, col=column_index, value=date_str) for row in rows]
    worksheet.update_cells(cell_list)  # Bulk update
    
# Variables
current_dir = os.path.dirname(os.path.abspath(__file__))
credentials_path = os.path.join(current_dir, 'lucid-cocoa-375621-2dc04e9671cb.json')
spreadsheet_url = 'https://docs.google.com/spreadsheets/d/1NR_EH89z9akyjJWslGYrUXN_gt3pxkbzFhW7FduVCdI/edit#gid=0'
SAVED_DATA_FILE = rf'{current_dir}/data.csv'
old_data = {}
if os.path.exists(SAVED_DATA_FILE):
    with open(SAVED_DATA_FILE, 'r', newline='', encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f)
        old_data = {rows[0]:rows[1] for rows in reader}


start_sheet = 6 ; end_sheet = 10            # 0 --> 10

for sheet_index in range(start_sheet, end_sheet):
    data_list = read_data_from_sheet(spreadsheet_url, sheet_index)
    rows_to_update = []
    
    with open(SAVED_DATA_FILE, 'a', newline='', encoding='utf-8', errors='ignore') as f:
        writer = csv.writer(f)
        for line in data_list:
            driver = init_driver(headless) # init a driver instance
            
            url = list(line.keys())[0]
            print(f'\n[{data_list.index(line) + 2}] Working on {url} ...')
            
            data = {}
            
            if '.pdf' in url[-4:]:
                try:
                    response = requests.get(url, timeout=60)
                    file = BytesIO(response.content)
                    pdf = PdfReader(file)
                    body_text = ""

                    # Iterate over the pages in the PDF and extract the text
                    for i in range(len(pdf.pages)):
                        body_text += normalize_and_hash(pdf.pages[i].extract_text())
                    
                    # check if the data is different
                    check_changes(old_data, body_text, url)
                    writer.writerow([url, body_text])
                except:
                    pass
                
            else:
                try:
                    go_to(driver, url)
                    
                    # get text from the first page
                    body_text = driver.find_element(By.TAG_NAME, "body").text
                    data[url] = normalize_and_hash(body_text)

                    # check if the data is different
                    check_changes(old_data, body_text, url)
                    writer.writerow([url, body_text])
                        
                    # other pages links
                    domain = urlparse(url).netloc # extract the domain of the main url
                    elements = driver.find_elements(By.TAG_NAME, "a") # find all the anchor tags

                    try:
                        same_domain_urls = [el.get_attribute("href") for el in elements if urlparse(el.get_attribute("href")).netloc == domain] # extract urls that have the same domain
                        same_domain_urls = list(set(same_domain_urls))

                        # get text from other pages
                        for each_href in same_domain_urls:
                            try:
                                go_to(driver, each_href)
                                
                                try:
                                    body_text = driver.find_element(By.TAG_NAME, "body").text
                                    data[url] = normalize_and_hash(body_text)
                                    
                                except:
                                    WebDriverWait(driver, 3).until(EC.alert_is_present())
                                    alert = driver.switch_to.alert
                                    alert.accept() ; sleep(2)
                                    
                                    body_text = driver.find_element(By.TAG_NAME, "body").text
                                    data[url] = normalize_and_hash(body_text)
                                
                                data[each_href] = body_text
                                
                                # check if the data is different
                                check_changes(old_data, body_text, each_href)
                                writer.writerow([each_href, body_text])
                                
                                if same_domain_urls.index(each_href) == 30:
                                    break
                            except:
                                pass
                    
                    except StaleElementReferenceException:
                        print('[!] StaleElementReferenceException')
                
                except StaleElementReferenceException:
                    print('[!] StaleElementReferenceException')
                    
                except NoSuchElementException:
                    pass

            # Instead of calling update_check_date, just keep track of which rows need to be updated.
            rows_to_update.append(data_list.index(line))
            
            driver.quit()
            
            sleep(60)
        
    # After all rows in this sheet have been processed, update the check dates.
    update_check_dates(spreadsheet_url, sheet_index, rows_to_update, date_format="%Y-%m-%d")

    
