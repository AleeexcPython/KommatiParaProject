**KommatiPara Data Processing Application**
**Overview**
The KommatiPara Data Processing Application is a Python script designed to merge and process two datasets related to KommatiPara's clients. The company, which deals with bitcoin trading, aims to collate client information and financial details for better client interfacing and marketing efforts.

Features
**Data Filtering:**
Filters clients based on specified countries (currently set for the United Kingdom and the Netherlands).

**Data Transformation:**
Removes personal identifiable information from the first dataset, excluding emails.
Removes credit card numbers from the second dataset.

****Data Joining:**
Joins the filtered datasets using the 'id' field.

**Column Renaming:**
Renames columns for better readability:
'id' to 'client_identifier'
'btc_a' to 'bitcoin_address'
'cc_t' to 'credit_card_type'

**Logging:**
Utilizes logging for informative messages during script execution.

**Output:**
Saves the processed data in a 'client_data' directory in the root of the project.

**Usage**
bash: python script.py <dataset_one_path> <dataset_two_path> <countries>
**dataset_one_path**: Path to the first dataset file.
**dataset_two_path**: Path to the second dataset file.
**countries**: Comma-separated list of countries for filtering clients.
**Example command-line**; python3 Abosolute file dataset_one  Absolute 
 file dataset_two "United Kingdom,Netherlands"

**Requirements**
Python 3.8
PySpark

**Setup**
Clone this repository.
Install the required dependencies (pip install pyspark).
Run the script with the appropriate command-line arguments.

**Output**
The processed data will be stored in the 'client_data' directory.

**Testing**
Test Script (test_script.py)
The test_script.py file contains unit tests for the functions filter_data and remove_personal_info in the client_data_processing module.

Test filter_data Function
This test ensures that the filter_data function correctly filters clients based on specified countries. It creates a Spark DataFrame with sample data, applies the filter, and asserts the equality of the resulting DataFrame with the expected DataFrame.

Test remove_personal_info Function
This test ensures that the remove_personal_info function correctly removes personally identifiable information from the input DataFrame. It creates a Spark DataFrame with sample data, applies the transformation, and asserts the equality of the resulting DataFrame with the expected DataFrame.

Use: *pytest test_script.py** in bash to test the script.  
