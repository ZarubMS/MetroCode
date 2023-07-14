from sqlite3 import Timestamp
import time
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import *
from croniter import croniter
from datetime import datetime, timedelta
import argparse
from py4j.java_gateway import java_import


#получение аргументов командной строки
def get_args():
    parser = createParser()
    namespace =  parser.parse_args()
    arguments = namespace.__dict__
    return arguments
        
def createParser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--date_from", default=datetime.now())
    parser.add_argument("-e", "--date_to", default=datetime.now()+timedelta(days=7))
    return parser

def get_data(from_table, columns):
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    df = spark.table(from_table).select(columns)
    return df

#Периодичность крона
@F.udf
def cron_type(cron):
    if cron.find('*') != -1:
        cron_list = cron.split()
        if (cron_list[1] == '*' or '/' in cron_list[1] or '/' in cron_list[0]) and ('/' not in cron_list[0]):
            periodicity = 'ежечасный'
        elif cron_list[2] != '*':
            periodicity = 'ежедневный'
        elif cron_list[2] == '*' and cron_list[1] != '*' and cron_list[4] == '*' and '/' not in cron_list[1]:
            periodicity = 'ежедневный' 
        elif cron_list[4] != '*':
            periodicity = 'еженедельный' 
        elif '/' in cron_list[0]:
            periodicity = 'ежеминутный'
        else:
            periodicity = 'неизвестно'
        return periodicity   
      





def run():

    #Получаем период из командной строки
    dt_fmt = '%Y-%m-%d %H:%M'
    args = get_args()
    date_from = datetime.strptime(args['date_from'] + ' '+'00:00', dt_fmt)
    date_to = datetime.strptime(args['date_to'] + ' '+'00:00', dt_fmt)
    

    #Получаем даги и крон в датафрейм
    columns = ['dag_id', 'is_paused', 'is_active', 'schedule_interval']
    df = get_data('public_stg.dag', columns)
    df = df.filter((df.is_paused  == 'false') & (df.is_active  == 'true'))
    df = df.drop('is_paused', 'is_active')
    df = df.select(df.dag_id.alias('dag_id_df'), df.schedule_interval)
    #Добавляем колонку с вычесленной периодичностью
    convertUDF = F.udf(lambda z: cron_type(z), StringType())
    df = df.withColumn('periodicity', cron_type(F.col('schedule_interval')))
    df = df.filter(F.col('periodicity') != 'ежеминутный')

    #Получаем даги, фактическое время работы и планируемый старт 
    columns = ['dag_id', 'start_date', 'end_date', 'execution_date']
    df1 = get_data('airflow_data.dag_run', columns)

    #Отфильтровываем даги которые попадают в период полученный из командной строки
    df1 = df1.withColumn('start_interval', F.lit(date_from)).withColumn('end_interval', F.lit(date_to))
    df1 = df1.filter((F.unix_timestamp('start_date') > F.unix_timestamp('start_interval')) & (F.unix_timestamp('end_date') <= F.unix_timestamp('end_interval')))


    #Добавляем колонку с планируемым временем окончания работы дага 
    df1 = df1.withColumn('execution_end_date', F.to_timestamp(F.from_unixtime(F.unix_timestamp('execution_date').cast('long') \
                                                                + F.unix_timestamp('end_date').cast('long') \
                                                                - F.unix_timestamp('start_date').cast('long'))))
    #Джойним (dag_id, start_date, end_date, execution_date, execution_end_date, schedule_interval, periodicity)
    df1 = df1.join(df, (df1.dag_id == df.dag_id_df), 'inner')
    df1 = df1.drop('dag_id_df')

    #Получаем даги и родителей 1ого порядка
    columns = ['dest_dag_nm', 'source_dag_nm']
    df2 = get_data('airflow_data.dependency_trees', columns)
    
    #Получаем даги и зависимые 1ого порядка
    df3 = df2.withColumnRenamed('dest_dag_nm', 'depend_dag_nm').withColumnRenamed('source_dag_nm', 'dest_dag_nm_1')
    df3 = df3.distinct().filter(F.col('dest_dag_nm_1').isNotNull())
    df3 = df3.groupBy('dest_dag_nm_1').agg(F.collect_list('depend_dag_nm').alias('depend_dags_nm'))
    df3 = df3.drop('depend_dag_nm')

    df2 = df2.distinct()
    df2 = df2.groupBy('dest_dag_nm').agg(F.collect_list('source_dag_nm').alias('source_dags_nm'))
    df2 = df2.drop('source_dag_nm')
    df2 = df2.orderBy('dest_dag_nm')

    #Джойним родителей и зависимые к (dag_id, start_date, end_date, execution_date, execution_end_date, schedule_interval, periodicity)
    df1 = df1.join(df2, (df1.dag_id == df2.dest_dag_nm), 'left').join(df3, (df1.dag_id == df3.dest_dag_nm_1), 'left')
    df1 = df1.drop('dest_dag_nm', 'dest_dag_nm_1')
        
    #Создаем датафрейм с дагами и планируемым временем
    df2 = df1.select(F.concat(F.col('dag_id'), F.lit('_'), F.lit('plan')).alias('dag_id'),
                     'execution_date',
                     'execution_end_date',
                     'schedule_interval',
                     'periodicity', 'source_dags_nm', 'depend_dags_nm')
    #Убираем лишние колонки
    df1 = df1.drop('execution_date', 'execution_end_date', 'start_interval', 'end_interval')

    #Обединяем планируемые и реальные даги в один датафрейм
    df1 = df1.union(df2)

    #Преобразуем в списки родительских и зависимых дагов строки 
    df1 = df1.withColumn('source_dags', F.concat_ws(' ', 'source_dags_nm')).withColumn('depend_dags', F.concat_ws(' ', 'depend_dags_nm'))
    df1 = df1.drop('source_dags_nm', 'depend_dags_nm')
    print(df1.count())

    

    
    



    
    #Записываем в вертику
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    gw = spark.sparkContext._gateway
    java_import(gw.jvm, "VerticaDialect")
    gw.jvm.org.apache.spark.sql.jdbc.JdbcDialects.registerDialect(gw.jvm.VerticaDialect())
    df1.write.format('jdbc') \
        .option("url", "jdbc:vertica://10.220.126.48:5433/") \
        .option("driver", "com.vertica.jdbc.Driver") \
        .option("dbtable", "public.dags_schedule") \
        .option("database", "TST") \
        .option("user", "dbadmin") \
        .option("password", "TSTpassword") \
        .mode("append") \
        .save()
    
    


if __name__ == '__main__':
    run()
    