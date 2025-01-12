from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Selenium setup
chrome_options = Options()
chrome_options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)


# Define the URL to parse
url = "https://airflow-gp.dwh.avo.uz/taskinstance/list/?_flt_0_state=failed&_flt_1_start_date=2024-12-04T19%3A00%3A00%2B00%3A00&_flt_4_task_id=%22odata_1c%22#"

# Open the page
driver.get(url)

try:
    # Wait for the table to load
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table.table"))
    )

    # Locate all rows in the task instance table
    rows = driver.find_elements(By.CSS_SELECTOR, "table.table tbody tr")

    failed_tasks = []

    for row in rows:
        # Check if the row represents a failed task
        state_cell = row.find_element(By.CSS_SELECTOR, "span.label[title='Current State: failed']")
        if state_cell:
            task_details = {
                "Dag Id": None,
                "Task Id": None,
                "Log URL": None,
            }

            # Get all cells in the row
            cells = row.find_elements(By.TAG_NAME, "td")

            if len(cells) > 0:
                # Extract DAG ID
                try:
                    dag_cell = cells[3].find_element(By.TAG_NAME, "a")
                    task_details["Dag Id"] = dag_cell.text.strip()
                except:
                    pass

                # Extract Task ID
                try:
                    task_cell = cells[4].find_element(By.TAG_NAME, "a")
                    task_details["Task Id"] = task_cell.text.strip()
                except:
                    pass

                # Extract Log URL
                try:
                    log_cell = cells[-1].find_element(By.TAG_NAME, "a")
                    if "tab=logs" in log_cell.get_attribute("href"):
                        task_details["Log URL"] = log_cell.get_attribute("href")
                except:
                    pass

            failed_tasks.append(task_details)

    # Print the extracted failed tasks
    for task in failed_tasks:
        print(f"Dag Id: {task['Dag Id']}, Task Id: {task['Task Id']}, Log URL: {task['Log URL']}")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the WebDriver
    driver.quit()
