from pyspark.sql import *
from pyspark.sql.functions import col, rank, max

spark = SparkSession \
    .builder \
    .appName("PySpark DataFrame") \
    .config("spark.jars", "postgresql-42.4.0.jar") \
    .getOrCreate()


def get_table(dbtable):
    return spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/demo").option(
        "dbtable", dbtable).option("user", "postgres").option("password", "1111").option("driver",
                                                                                         "org.postgresql.Driver").load()


def task1():
    dbtable = 'film_category'
    df_film_category = get_table(dbtable)
    dbtable = 'category'
    df_category = get_table(dbtable)
    df = df_film_category.join(df_category, df_category.category_id == df_film_category.category_id).drop(
        df_category['category_id'])
    df.groupBy('name').count().orderBy(col('count').desc()).show()


def task2():
    df_actor = get_table(dbtable='actor')
    df_film_actor = get_table(dbtable='film_actor')
    df_inventory = get_table(dbtable='inventory')
    df_rental = get_table(dbtable='rental')
    df = df_actor.join(df_film_actor, df_film_actor['actor_id'] == df_actor['actor_id'])
    df = df.join(df_inventory, df_inventory['film_id'] == df['film_id'])
    df = df.join(df_rental, df_rental['inventory_id'] == df['inventory_id'])
    df = df.groupBy(['first_name', 'last_name']).count()
    df = df.orderBy(col('count').desc())
    df.show(10)


def task3():
    df_category = get_table(dbtable='category')
    df_film_category = get_table(dbtable='film_category')
    df_inventory = get_table(dbtable='inventory')
    df_rental = get_table(dbtable='rental')
    df_payment = get_table(dbtable='payment')
    df_film = get_table(dbtable='film')
    df = df_category.join(df_film_category, df_category['category_id'] == df_film_category['category_id']).drop(
        df_category['category_id'])
    df = df.join(df_film, df['film_id'] == df_film['film_id']).drop(df['film_id'])
    df = df.join(df_inventory, df_inventory['film_id'] == df['film_id']).drop(df['film_id'])
    df = df.join(df_rental, df['inventory_id'] == df_rental['inventory_id']).drop(df['inventory_id'])
    df = df.join(df_payment, df['rental_id'] == df_payment['rental_id']).drop(df['rental_id'])
    df = df.groupBy(df['name']).agg({'amount': 'sum'}).withColumnRenamed('sum(amount)', 'amount')
    df.orderBy(col('amount').desc()).show(1)


def task4():
    df_film = get_table(dbtable='film')
    df_inventory = get_table(dbtable='inventory')
    df = df_film.join(df_inventory, df_inventory['film_id'] == df_film['film_id'], how='left')
    df = df.where(col('inventory_id').isNull()).select(col('title'))
    df.show(df.count())


def task5():
    df_actor = get_table(dbtable='actor')
    df_film_actor = get_table(dbtable='film_actor')
    df_category = get_table(dbtable='category')
    df_film_category = get_table(dbtable='film_category')

    df = df_actor.join(df_film_actor, on=['actor_id'])
    df = df.join(df_film_category, on=['film_id'])
    df = df.join(df_category, on=['category_id'])
    df = df.where(col('name') == 'Children')
    df = df.groupBy(col('actor_id')).count()
    df = df.withColumn('rank', rank().over(Window.orderBy(col('count').desc()))).where(col('rank') <= 3)
    df = df.join(df_actor, on='actor_id').select('first_name', 'last_name')
    df.show(df.count())


def task6():
    df_city = get_table(dbtable='city')
    df_address = get_table(dbtable='address')
    df_customer = get_table(dbtable='customer')
    df = df_city.join(df_address, on='city_id')
    df = df.join(df_customer, on='address_id')
    df_active = df.where(col('active') == 1).groupBy(col('city_id')).count().withColumnRenamed('count',
                                                                                               'active_counter')
    df_nonactive = df.where(col('active') == 0).groupBy(col('city_id')).count().withColumnRenamed('count',
                                                                                                  'nonactive_counter')
    df = df.join(df_active, on='city_id', how='left')
    df = df.join(df_nonactive, on='city_id', how='left')
    df = df.select('city', 'active_counter', 'nonactive_counter')
    df = df.na.fill(value=0)
    df = df.orderBy(col('nonactive_counter').desc())
    df.show(df.count())


def task7():
    df_category = get_table(dbtable='category')
    df_film_category = get_table(dbtable='film_category')
    df_inventory = get_table(dbtable='inventory')
    df_rental = get_table(dbtable='rental')
    df_customer = get_table(dbtable='customer')
    df_address = get_table(dbtable='address')
    df_city = get_table(dbtable='city')
    df_film = get_table(dbtable='film')
    df = df_category.join(df_film_category, on='category_id')
    df = df.join(df_film, on='film_id')
    df = df.join(df_inventory, on='film_id')
    df = df.join(df_rental, on='inventory_id')
    df = df.join(df_customer, on='customer_id')
    df = df.join(df_address, on='address_id')
    df = df.join(df_city, on='city_id')
    df = df.where(col('title').rlike('^A') & col('city').rlike('-'))
    df = df.groupBy('title', 'city', 'name').agg({'rental_date': 'sum', 'return_date': 'sum'})
    df = df.select('name', col('sum(return_date)') - col('sum(rental_date)')).withColumnRenamed(
        '(sum(return_date) - sum(rental_date))', 'rent_hours')
    max_rent = df.select(max('rent_hours')).withColumnRenamed('max(rent_hours)', 'max').collect()[0][0]
    df = df.where(col('rent_hours') == max_rent).select('name')
    df.show()

