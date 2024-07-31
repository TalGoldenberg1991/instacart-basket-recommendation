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




# Path to the CSV file
#The os.path.join function in Python is used to concatenate multiple components of a file path in a way that is appropriate for the operating system you are working on. This helps to ensure that the paths are constructed correctly and are portable across different operating systems
orders_csv_file_path = os.path.join(orders_extract_path, 'orders.csv')
products_csv_file_path = os.path.join(products_extract_path, 'products.csv')
order_products__train_csv_file_path = os.path.join(order_products__train_extract_path,'order_products__train.csv')
order_products__prior_csv_file_path = os.path.join(order_products__prior_extract_path,'order_products__prior.csv')
departments_csv_file_path = os.path.join(departments_extract_path, 'departments.csv')
aisles_csv_file_path = os.path.join(aisles_extract_path, 'aisles.csv')


# Load the CSV file into a pandas DataFrame
orders_df = pd.read_csv(orders_csv_file_path)
products_df = pd.read_csv(products_csv_file_path)
order_products__train_df = pd.read_csv(order_products__train_csv_file_path)
order_products__prior_df = pd.read_csv(order_products__prior_csv_file_path)
departments_df = pd.read_csv(departments_csv_file_path)
aisles_df = pd.read_csv(aisles_csv_file_path)

# Load the CSV files into Spark DataFrames
orders_df_spark = spark.read.csv(orders_csv_file_path, header=True, inferSchema=True)
products_df_spark = spark.read.csv(products_csv_file_path, header=True, inferSchema=True)
order_products__train_df_spark = spark.read.csv(order_products__train_csv_file_path, header=True, inferSchema=True)
order_products__prior_df_spark = spark.read.csv(order_products__prior_csv_file_path, header=True, inferSchema=True)
departments_df_spark = spark.read.csv(departments_csv_file_path, header=True, inferSchema=True)
aisles_df_spark = spark.read.csv(aisles_csv_file_path, header=True, inferSchema=True)


#the main objective is to predict the 5 items the user will buy in his next purchase

#why 5? I assume that the recomendation component will be used for max 5 items. The user will probably won't utilize the recommendation component for more than 5 items. this shpuld be validated via historical recommendation data / a/b test

#what will greatly effect the desired products for a user ?
#if the item were purchased before
#the number of times the item was purchased before
#the order in which the user purchased the item , if he purchased it among the first items, it indicates it is more likely the user will purchase it again (assumption, needs validation)
#the week day and the time of his next order


#Data Merging
joined_df = orders_df_spark.join(order_products__prior_df_spark, on="order_id", how="inner")
joined_df = joined_df.join(products_df_spark, on = "product_id", how = "inner")
joined_df.show(5)




#Prepare Data for Model -->Transform the data into a format suitable for training the ALS model.
#why als? since is well-suited for basket prediction due to several reasons:
#ALS uses collaborative filtering, which leverages user behavior data to make predictions. It identifies patterns in users' purchasing behaviors to recommend items that similar users have purchased.
#ALS can handle implicit feedback (e.g., purchase history)
#ALS is designed to scale to large datasets
#ALS factorizes the user-item interaction matrix, capturing latent factors that explain purchase patterns. This helps in predicting items likely to be bought together.
#Cold Start Handling: ALS has strategies to handle new users or items (cold start problem), ensuring robust recommendations even with sparse data.


# Group by user_id and product_id to count the number of purchases
from pyspark.sql.functions import count

purchase_counts = joined_df.groupBy("user_id", "product_id").agg(count("*").alias("purchase_count"))



#split to test & train
train_df, test_df = purchase_counts.randomSplit([0.8, 0.2], seed=12345)

from pyspark.ml.recommendation import ALS

# Define the ALS model
als = ALS(userCol="user_id", itemCol="order_id", ratingCol="purchases", coldStartStrategy="drop")

# Train the model on the training DataFrame
als_model = als.fit(train_df)

# Show the model summary
als_model


# Initialize ALS model
als = ALS(
    userCol='user_id',
    itemCol='product_id',
    ratingCol='purchase_count',
    rank=10,#Meaning: The number of latent factors to use in the matrix factorization. It determines the number of features to learn for each user and item.
    maxIter=10,
    regParam=0.1,#The regularization parameter, which controls the degree of regularization applied to the model to prevent overfitting. A higher value implies stronger regularization.
    coldStartStrategy='drop'
)

# Train the model
als_model = als.fit(purchase_counts)
