from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
from bs4 import BeautifulSoup

# constants
url = 'https://campus.quipper.com/directory?location=Jawa%20Timur'

# Initialize Chrome driver instance
options = Options()
options.add_experimental_option("detach", True) # supaya g otomatis ketutup windownya
driver = webdriver.Chrome(service=ChromeService(executable_path=ChromeDriverManager().install()), options=options)

# Navigate to the url
driver.get(url)

# load all cards ---------------------------------------------------------------------------------
while True:
    try:
        # Find the div based on its exact text content
        # This is often more reliable than using class names
        load_more_div = driver.find_element(By.XPATH, "//div[text()='Lihat kampus lain']")
        
        # Click the div
        load_more_div.click()
        print("Clicked the 'Lihat kampus lain' div...")
        
        # Wait for 2.5 seconds for new content to load
        time.sleep(2.5)

    except NoSuchElementException:
        # This error means the div was not found, so we can stop clicking
        print("Button not found. Assuming all content has been loaded.")
        break

print("\nFinished loading all content.")

# -------------------------------------------------------------------------------------------------------------





# get the links of the universities ---------------------------------------------------------------------------------
html_content = driver.page_source

soup = BeautifulSoup(html_content, 'html.parser')
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
            print(link)
        
        links_list.append(link)
        

print(f"\nSuccessfully extracted {len(links_list)} links.")
# -------------------------------------------------------------------------------------------------------------



# Close the driver
# driver.quit()