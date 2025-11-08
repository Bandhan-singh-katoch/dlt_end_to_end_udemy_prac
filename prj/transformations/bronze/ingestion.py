from pyspark import pipelines as dp


@dp.table(
    name = "bs_sales_bronze"
)
def bs_sales_bronze():
    return (
        spark.readStream.format('cloudFiles')
            .option('cloudFiles.format','csv')
            .load("/Volumes/enterprise_dev_nonprod/ja_dev/bs_bronze/sales/")
    )

@dp.table(
    name = "bs_stores_bronze"
)
def bs_stores_bronze():
    return (
        spark.readStream.format('cloudFiles')
            .option('cloudFiles.format','csv')
            .load("/Volumes/enterprise_dev_nonprod/ja_dev/bs_bronze/stores/")
    )

@dp.table(
    name = "bs_products_bronze"
)
def bs_products_bronze():
    return (
        spark.readStream.format('cloudFiles')
            .option('cloudFiles.format','csv')
            .load("/Volumes/enterprise_dev_nonprod/ja_dev/bs_bronze/products/")
    )

@dp.table(
    name = "bs_customers_bronze"
)
def bs_customers_bronze():
    return (
        spark.readStream.format('cloudFiles')
            .option('cloudFiles.format','csv')
            .load("/Volumes/enterprise_dev_nonprod/ja_dev/bs_bronze/customers/")
    )


