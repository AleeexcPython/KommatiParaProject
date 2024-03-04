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

**Requirements**
Python 3.8
PySpark

**Setup**
Clone this repository.
Install the required dependencies (pip install pyspark).
Run the script with the appropriate command-line arguments.

**Output**
The processed data will be stored in the 'client_data' directory.

