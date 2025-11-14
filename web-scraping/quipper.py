import pandas as pd
import time
import json
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup


# constants
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
BASE_PATH = os.path.join(parent_dir, "csv_result") + os.sep

url = 'https://campus.quipper.com/directory?'
debug = False
debug_iteration = 2
pd.set_option('display.max_columns', None)
MAX_WAIT_TIME = 10  # seconds

# Initialize Chrome driver instance
options = Options()
options.add_experimental_option("detach", True) # supaya g otomatis ketutup windownya
driver = webdriver.Chrome(service=ChromeService(executable_path=ChromeDriverManager().install()), options=options)

# Navigate to the url
driver.get(url)

# load all cards ---------------------------------------------------------------------------------
debug_count=0
while True:

    if(debug): # for testing purposes, limit to first 5 clicks
        debug_count+=1
        if(debug_count>debug_iteration):
            print("Debug mode: Stopping after 5 iterations.")
            break

    try:
        # Find the div based on its exact text content
        # This is often more reliable than using class names
        try:
            # Attempt to find the element using the most specific locator
            pop_up_close_button = driver.find_element(By.CLASS_NAME, 'popup-close')
            
            # If the element is found, proceed to check if it's displayed and click it
            if pop_up_close_button.is_displayed():
                pop_up_close_button.click()
                print("Closed pop-up window.")
                time.sleep(1) 

        except NoSuchElementException:
            # If the element is NOT found, the exception is caught, and we do nothing
            print("Pop-up close button did not exist or was not found.")
            pass
            
        wait = WebDriverWait(driver, MAX_WAIT_TIME) # Wait up to 10 seconds
        load_more_div = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//div[text()='Lihat kampus lain']"))
        )

        # driver.execute_script("arguments[0].scrollIntoView(true);", load_more_div)
        
        # Click the div
        # load_more_div.click()
        driver.execute_script("arguments[0].click();", load_more_div)
        print("Clicked the 'Lihat kampus lain' div...")
        
        # Wait for 2.5 seconds for new content to load
        time.sleep(1.25)

    except NoSuchElementException:
        # This error means the div was not found, so we can stop clicking
        print("Button not found. Assuming all content has been loaded.")
        break
    except TimeoutException:
        # This means the button was not found or did not become clickable within MAX_WAIT_TIME.
        # This is the expected way to stop when all content is loaded.
        print(f"Button did not become clickable within {MAX_WAIT_TIME} seconds. Assuming all content is loaded.")
        break
    except Exception as e:
        # Catch any other unexpected error (like ElementNotInteractable after clicking)
        print(f"An unexpected error occurred: {e}. Stopping load.")
        break

    

print("\nFinished loading all content.")

# -------------------------------------------------------------------------------------------------------------





# get the links of the universities ---------------------------------------------------------------------------------
html_content = driver.page_source

soup = BeautifulSoup(html_content, 'html.parser')

image_card_elements = soup.find_all(class_='campus-card-img')

# 2. Get the count by finding the length of the list
count = len(image_card_elements)

print(f"{count} cards found.")

a_tags = soup.find_all('a')
links_list = []


print("--- Extracting all links ---")
for tag in a_tags:
    # Use .get('href') to avoid errors if a tag has no href
    link = tag.get('href')
    
    # Check if the link was found and is not just a '#'
    if link and link != '#':
        # Optional: Handle relative URLs by joining them with the base URL
        if link.startswith('/directory'):
            link = "https://campus.quipper.com" + link
            links_list.append(link)
        
        
        
print(f"\nSuccessfully extracted {len(links_list)} links.")
# -------------------------------------------------------------------------------------------------------------





# end result are 3 dataframes:
# 1. institution_code (act as primary key, quipper-[increasing integer]) | institution_name | body_type (negeri or swasta) | link | fee | student_amount | lecturer_amount | contact(json) | description
# 2. quipper_prodi_code (act as primary key, quipper-[increasing integer]) | faculty | prodi  | PK_on_dataframe_one
# 3. quipper_faculty_code (act as primary key, quipper-[increasing integer]) | faculty | address | PK_on_dataframe_one

def get_text(soup_object, tag=None, class_name="", find_all=False):
    """
    Finds a single element and returns its cleaned text.
    Returns None if the element is not found.
    """
    texts = None
    text = None
    if(find_all):
        texts = soup_object.find_all(tag, class_=class_name)
    else:
        text = soup_object.find(tag, class_=class_name)
    

    if texts:
        return [t.text.strip() for t in texts]
    elif text:
        return text.text.strip()
    return None




# lists for dataframes
data_institution = []
data_prodi = []
data_faculty = []

print(links_list)

# iterate through each link to get the data
for idx,link in enumerate(links_list):
    
    driver.get(link)
    html_content = ''

    try:
        # Wait until a specific, critical element on the new page is visible.
        # We choose the 'school-page-banner__title' class as it holds the university name.
        WebDriverWait(driver, MAX_WAIT_TIME).until(
            EC.visibility_of_element_located((By.CLASS_NAME, 'school-page-banner__title'))
        )
        
        # Execution continues immediately when the element is visible (no wasted time).
        html_content = driver.page_source
        
    except TimeoutException:
        print(f"ERROR: Page content failed to load within {MAX_WAIT_TIME} seconds for link: {link}")
        # Handle the error gracefully (e.g., set soup to None and skip data extraction for this link)
        soup = None
        pass



    # save the html for scraping
    soup = BeautifulSoup(html_content, 'html.parser')


    # for df_institution ---------------------------------------------------------------------------
    data_institution_row = {}
    data_institution_row['institution_code'] = 'quipper-' + str(idx + 1)
    data_institution_row['institution_name'] = get_text(soup, 'h1', 'school-page-banner__title')
    data_institution_row['body_type'] = get_text(soup, 'p', 'school-page-banner__subbody')

    description_p = soup.select_one('div.school-profile__description > p')
    if description_p:
        data_institution_row['description'] = description_p.text.strip()
    else:
        print("Description paragraph not found.")

    data_institution_row['link'] = link
    #---
    keys = get_text(soup, class_name='school-profile__detail-key', find_all=True)
    values = get_text(soup, class_name='school-profile__detail-val', find_all=True)
    
    # turn to dictionary for better access
    info_dict = dict(zip(keys, values))
    data_institution_row['accred'] = info_dict.pop('Akreditasi', None)
    data_institution_row['fee'] = info_dict.pop('Biaya Kuliah', None)
    data_institution_row['student_amount'] = info_dict.pop('Siswa', None)
    data_institution_row['lecturer_amount'] = info_dict.pop('Dosen', None)
    # get contacts, turn to json
    contact_dict = {}
    contact_dict['website'] = info_dict.pop('Website', None)
    contact_dict['email'] = info_dict.pop('Email', None)
    contact_dict['phone'] = info_dict.pop('Telepon', None)
    data_institution_row['contact'] = json.dumps(contact_dict)
    data_institution_row['unknown_field'] = json.dumps(info_dict) # save the remaining info in json format

    data_institution.append(data_institution_row)
    # -------------------------------------------------------------------------------------------------------------

    # for df_prodi ------------------------------------------------------------------------------------------------
    faculty_blocks = soup.find_all(class_='faculty-item__content')

    for block in faculty_blocks:
        faculty_name = block.find(class_='faculty-item__name').text.strip()
        

        subject_items = block.find_all(class_='faculty-subjects__item')
        
        for subject in subject_items:
            data_prodi.append((faculty_name, subject.text.strip(), data_institution_row['institution_code'])) # (faculty, prodi, institution_code)
        

    # -------------------------------------------------------------------------------------------------------------

    # for df_faculty ----------------------------------------------------------------------------------------------
    location_blocks = soup.find_all(class_='school-locations__list-item')        

    for block in location_blocks:
        # print(block.prettify())
        campus_name = get_text(block, class_name='school-locations__campus-name')
        address = get_text(block, class_name='text-variant-body')
        faculties = get_text(block, class_name='school-locations__faculty-link', find_all=True)

        if not faculties or len(faculties) == 0:
            data_faculty.append(("ALL", campus_name, address, data_institution_row['institution_code']))
            continue

        for faculty_name in faculties:
            data_faculty.append((faculty_name, campus_name, address, data_institution_row['institution_code'])) # (faculty, prodi, institution_code)
    

    
    # -------------------------------------------------------------------------------------------------------------

# Close the driver
driver.quit()

df_institution = pd.DataFrame(data_institution, columns=['institution_code', 'institution_name', 'body_type', 'link', 'fee', 'student_amount', 'lecturer_amount', 'contact', 'description', 'unknown_field'])
df_prodi = pd.DataFrame(data_prodi, columns=['faculty', 'prodi', 'institution_code'])
df_faculty = pd.DataFrame(data_faculty, columns=['faculty', 'building_name', 'address', 'institution_code']) 


#    index=False prevents Pandas from writing the DataFrame index as a column
df_institution.to_csv(os.path.join(BASE_PATH, 'quipper_institution.csv'), index=False)
df_prodi.to_csv(os.path.join(BASE_PATH, 'quipper_prodi.csv'), index=False)
df_faculty.to_csv(os.path.join(BASE_PATH, 'quipper_faculty.csv'), index=False)