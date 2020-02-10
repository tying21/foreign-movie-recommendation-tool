# -*- coding: utf-8 -*-
from os.path import abspath
from configparser import ConfigParser
import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql import SparkSession


class MovieTransformer(object):
    """
    class that reads data from S3 bucket, process with Spark
    and saves the movie match-up table to S3 as .parquet file, and movie look-up table to PostgreSQL database
    """

    def __init__(self, colname, newname, country, genre, director, cast, df):
        """
        # colname, newname: string (old col: "title", new col name)
        # df: movie data frame read from S3 bucket
        # return:
        """
        self.colname = colname
        self.newname = newname
        self.country = country
        self.genre = genre
        self.director = director
        self.cast = cast
        self.df = df

    def filter(self):
        """
        filter out the rows with null value for col “country”
        filter out the rows with null value for col “director” & "cast"
        """
        newdf = self.df.filter(self.df.country.isNotNull())
        newdf = newdf.filter((newdf.cast.isNotNull())|(newdf.director.isNotNull()))

        return newdf

    def format(self, df):
        """
        1. remove all punctuation marks
        2. make strings lowercase and strip the space
        3. add hyphen between words
        :return: df with new col "movie_title" added, formatted as in "xxx-xxx-xxx"
        """
        newdf = self.df.withColumn(self.newname, f.regexp_replace(f.col(self.colname), "\p{P}", ""))
        newdf = newdf.withColumn(self.newname, f.lower(f.trim(f.col(self.newname))))
        newdf = newdf.withColumn(self.newname, f.regexp_replace(f.col(self.newname), " ", "-"))
        return newdf

    def explode_lookup(self, df):
        """
        make col "director" & "cast" lowercase
        convert columns “director” & "cast" into arrays of strings
        :return: df with "cast" & "director" exploded
        """
        newdf = df.withColumn(self.cast, f.lower(f.col(self.cast)))
        newdf = newdf.withColumn(self.director, f.lower(f.col(self.director)))
        newdf = newdf.withColumn(self.cast,
                              f.split(f.col(self.cast), ",\s*").cast(ArrayType(StringType())).alias(self.cast))
        newdf = newdf.withColumn(self.director,
                              f.split(f.col(self.director), ",\s*").cast(ArrayType(StringType())).alias(self.director))
        newdf = newdf.withColumn(self.cast, f.explode(f.col(self.cast)))
        newdf = newdf.withColumn(self.director, f.explode(f.col(self.director)))
        return newdf

    def explode_db(self, df):
        """
        convert “country” string type into array of strings
        convert “listed_in” string type into array of strings
        :return: df with "country" & "listed_in" exploded
        """
        newdf = df.withColumn(self.country, f.split(f.col(self.country), ",\s*").cast(ArrayType(StringType())).alias(self.country))
        newdf = newdf.withColumn(self.country, f.explode(f.col(self.country)))
        newdf = newdf.withColumn(self.genre, f.split(f.col(self.genre), ",\s*").cast(ArrayType(StringType())).alias(self.genre))
        newdf = newdf.withColumn(self.genre, f.explode(f.col(self.genre)))
        return newdf


class Database(object):
    """
    class constructor that saves the spark DataFrame to PostgreSQL
    """

    def __init__(self):
        self.username = config.get('PostgreSQL', 'username')
        self.password = config.get('PostgreSQL', 'password')

        instance = config.get('PostgreSQL', 'instance')
        database = config.get('PostgreSQL', 'database')
        self.url = 'jdbc:postgresql://{}:5432/{}'.format(instance, database)

    def save(self, data, table, mode='append'):
        data.write.format("jdbc") \
            .option("url", self.url) \
            .option("dbtable",table) \
            .option("user", self.username) \
            .option("password",self.password) \
            .option("driver", "org.postgresql.Driver") \
            .mode(mode).save()

class ReadCSV(object):

    def __init__(self,folder_name, file_name):
        self.folder_name = folder_name
        self.file_name = file_name
        self.bucket = config.get('AWS', 'bucket')
        self.path = 's3a://{}/{}/{}'.format(self.bucket, self.folder_name, self.file_name)

    def load(self):
        print(self.path)
        csv = spark.read.csv(self.path, header=True)
        return csv

    def save_to_parquet(self, df, name):
        df.write.parquet("s3a://{}/{}/".format(self.bucket, self.folder_name, self.file_name) + name)


if __name__ == '__main__':
    # -- Load Configuration --
    config = ConfigParser()
    config.read(abspath('config.ini'))
    # -- Init Spark --
    bucket = config.get('AWS', 'bucket')
    jar = config.get('AWS', 'jar')
    spark = SparkSession.builder.appName(bucket).config("spark.jars", jar).getOrCreate()
    # -- Process Data --
    folder_name = "netflix"
    file_name = "netflix_titles.csv"
    csv_task = ReadCSV(folder_name, file_name)
    df = csv_task.load()
    # -- Save movie match-up table to S3 as .parquet
    movie_db = MovieTransformer("title", "movie_title", "country", "listed_in", "director", "cast", df)
    newdb = movie_db.filter()
    newdb = movie_db.format(newdb)
    lookup = movie_db.explode_lookup(newdb)
    parquet_file = "movie_lookup.parquet"
    csv_task.save_to_parquet(lookup, parquet_file)
    # --Save move look-up table to PostgreSQL
    newdb = movie_db.explode_db(newdb)
    newdb = newdb.toDF('show_id', 'type', 'title', 'director', 'cast', 'country', 'date_added', 'release_year', 'rating', 'duration', 'listed_in', 'description', 'movie_title')
    postgres=Database()
    postgres.save(newdb, table="public.newdb")