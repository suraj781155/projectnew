from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,TimestampType

spark =SparkSession.builder.master("local[*]") \
                           .appName("myapp") \
                            .getOrCreate()

schemastruct = StructType([StructField('browser',StringType(),True),
                    StructField('city',StringType(),True),
                    StructField('click_event_id',IntegerType(),True),
                    StructField('country',StringType(),True),
                    StructField('device',StringType(),True),
                    StructField('ip_address', StringType(),True),
                    StructField('timestamp', TimestampType(),True),
                    StructField('url', StringType(),True),
                    StructField('user_id', IntegerType(),True)
                   ])
df=spark.read.format("json") \
              .option("schema",schemastruct) \
             .load("C:/Users/Suraj/Downloads/DE_assignment/user_click_data.json.json")

df.createOrReplaceTempView("temp")

df1=spark.sql(""" select country,url,
                 to_date(timestamp) as event_date,
                 count(click_event_id) as click_count,
                 avg(temp_spent) as average_minutes_spent
                 ,count(distinct user_id) as unique_users_count from (select country,city,click_event_id,url,user_id,timestamp,
                  minute(timestamp)-lead(minute(timestamp),1,0) 
                over(partition by country,city,user_id 
                order by country,city,user_id ,minute(timestamp) desc) as temp_spent
                from temp order by country,city,user_id ,timestamp) src group by country,url,to_date(timestamp)""")
df1.show()








