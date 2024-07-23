from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ProductsAndCategories").getOrCreate()

products_data = [
    (1, "Product A"),
    (2, "Product B"),
    (3, "Product C"),
    (4, "Product D")
]

categories_data = [
    (1, "Category X"),
    (2, "Category Y"),
    (3, "Category Z")
]

product_category_data = [
    (1, 1),
    (1, 2),
    (2, 3),
    (3, 1)
]

products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])

categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])

product_category_df = spark.createDataFrame(product_category_data, ["product_id", "category_id"])

def get_products_and_categories(products_df, categories_df, product_category_df):
    product_category_joined = product_category_df \
        .join(products_df, "product_id") \
        .join(categories_df, "category_id") \
        .select(products_df.product_name, categories_df.category_name)
    
    products_with_categories = product_category_df.select("product_id").distinct()
    products_without_categories = products_df.join(products_with_categories, "product_id", "left_anti") \
        .select(products_df.product_name)
    
    return product_category_joined, products_without_categories

product_category_pairs, products_without_categories = get_products_and_categories(products_df, categories_df, product_category_df)

print("Пары 'Имя продукта – Имя категории':")
product_category_pairs.show()

print("Продукты без категорий:")
products_without_categories.show()

spark.stop()