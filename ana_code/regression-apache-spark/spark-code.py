# Set up instructions:
# module purge
# module load python/gcc/3.7.9 
# pyspark --deploy-mode client

## Regression part inspired by https://towardsdatascience.com/building-a-linear-regression-with-pyspark-and-mllib-d065c3ba246a



# reading part
real = spark.read.csv("final/input/real_cleaned.csv")

# rename real columns
actual_columns = ["date", "zipcode", "median_listing_price", "active_listing_count",
                  "median_per_sq_ft", "median_sq_ft", "average_listing_price",
                  "total_listing_count"]

for i in range(len(actual_columns)):
    real = real.withColumnRenamed("_c" + str(i), actual_columns[i])

real.show(5)

income = spark.read.csv("final/input/income_cleaned.csv")

income_columns = ["zipcode", "state", "agi_stub", "n1", "total_income", "state_and_local",
                 "real_estate", "num_paid", "paid_amount", "taxable_amount"]

for i in range(len(income_columns)):
    income = income.withColumnRenamed("_c" + str(i), income_columns[i])

income.show(5)

# establish SQL
# enable the tables to be used by SQL
real.createOrReplaceTempView("real_estate_clean")
income.createOrReplaceTempView("income_clean")

# example dataframe
df = spark.sql("SELECT * from income_clean")
df.show(2)

## We need to get a new table by aggregating the two tables and then joining them

query1 = """

WITH income AS (
	
	SELECT zipcode, state, sum(total_income*1000)/sum(n1) as avg_income from income_clean GROUP BY zipcode, state
),

real_estate AS (

	SELECT zipcode, avg(median_listing_price) as avg_median_listing_price from real_estate_clean GROUP BY zipcode
)

SELECT income.zipcode, income.state, income.avg_income, real_estate.avg_median_listing_price, 
income.avg_income - real_estate.avg_median_listing_price as diff from income JOIN real_estate ON 
income.zipcode = real_estate.zipcode ORDER BY diff


"""

query1 = query1.replace("\n", " ").replace("\t", " ")


# this query joins the two datasets, aggregates them by zipcode, and selects only the relevant columns
query2 = """
WITH income AS (
	
	SELECT zipcode, state, (sum(total_income)-sum(state_and_local))*1000/sum(n1) as post_tax_income from income_clean GROUP BY zipcode, state
),

real_estate AS (

	SELECT zipcode, avg(median_listing_price) as avg_median_listing_price from real_estate_clean GROUP BY zipcode
)

SELECT income.zipcode, income.state, income.post_tax_income, real_estate.avg_median_listing_price, income.post_tax_income - real_estate.avg_median_listing_price as diff from income JOIN real_estate ON income.zipcode = real_estate.zipcode ORDER BY diff



"""

query2 = query2.replace("\n", " ").replace("\t", " ")

df = spark.sql(query2)
df.show()



#### Regression Part
## Inspired by https://towardsdatascience.com/building-a-linear-regression-with-pyspark-and-mllib-d065c3ba246a
# Let's do a simple regression
# Let's just see what the median_listing_prices should be, if we only use post_tax income as a factor
from pyspark.ml.feature import VectorAssembler

vectorAssembler = VectorAssembler(inputCols = ["post_tax_income"], outputCol = "features")

vdf = vectorAssembler.transform(df)
vdf = vdf.select(['features', 'avg_median_listing_price'])

vdf.show()

splits = vdf.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]

from pyspark.ml.regression import LinearRegression

lr = LinearRegression(featuresCol = 'features', labelCol = 'avg_median_listing_price', 
                      maxIter = 10, regParam = 0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_df)

print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))


# is there an actual correlation here?
trainingSummary = lr_model.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)


train_df.describe().show()

lr_predictions = lr_model.transform(test_df)
lr_predictions.select("prediction", "avg_median_listing_price", "features").show(5)

from pyspark.ml.evaluation import RegressionEvaluator
lr_evaluator = RegressionEvaluator(predictionCol = "prediction",
                                  labelCol = "avg_median_listing_price",
                                  metricName = "r2")

print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(lr_predictions))

