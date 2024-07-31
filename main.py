import pandas as pd
import zipfile

# Path to your zipped CSV file in Google Drive
zip_file_path = '/content/drive/MyDrive/data projects/instacart/instacart-market-basket-analysis.zip'
import zipfile
import os

# Path to the instacart zip file
zip_file_path = '/content/drive/MyDrive/instacart-market-basket-analysis.zip'

# Extraction directory for instacart-market-basket-analysis.zip
extract_path = '/content/instacart_extracted/'
os.makedirs(extract_path, exist_ok=True)

# Extract the main zip file
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

# Path to the zip files within the extracted content
orders_zip_file_path                 = os.path.join(extract_path, 'orders.csv.zip')
products_zip_file_path              = os.path.join(extract_path, 'products.csv.zip')
order_products__train_zip_file_path = os.path.join(extract_path, 'order_products__train.csv.zip')
#order products --> These files specify which products were purchased in each order
order_products__prior_zip_file_path = os.path.join(extract_path, 'order_products__prior.csv.zip')
 #order_products__prior.csv contains previous order contents for all customers.
#'reordered' indicates that the customer has a previous order that contains the product.
departments_zip_file_path           = os.path.join(extract_path, 'departments.csv.zip')
aisles_zip_file_path                = os.path.join(extract_path, 'aisles.csv.zip')



# create the directory for xx.csv.zip in colab
orders_extract_path = '/content/orders_extracted/'
os.makedirs(orders_extract_path, exist_ok=True)

products_extract_path = '/content/products_extracted/'
os.makedirs(products_extract_path, exist_ok=True)

order_products__train_extract_path = '/content/order_products__train_extracted/'
os.makedirs(order_products__train_extract_path, exist_ok=True)

order_products__prior_extract_path = '/content/order_products__prior_extracted/'
os.makedirs(order_products__prior_extract_path, exist_ok=True)

departments_extract_path = '/content/departments_extracted/'
os.makedirs(departments_extract_path, exist_ok=True)

aisles_extract_path = '/content/aisles_extracted/'
os.makedirs(aisles_extract_path, exist_ok=True)



# extracting the contents of a ZIP file to the directory we created
with zipfile.ZipFile(orders_zip_file_path, 'r') as zip_ref:
   zip_ref.extractall(orders_extract_path)

with zipfile.ZipFile(products_zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(products_extract_path)

with zipfile.ZipFile(order_products__train_zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(order_products__train_extract_path)

with zipfile.ZipFile(order_products__prior_zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(order_products__prior_extract_path)

with zipfile.ZipFile(departments_zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(departments_extract_path)


with zipfile.ZipFile(aisles_zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(aisles_extract_path)
