# -*- coding: utf-8 -*-
from os.path import abspath
from configparser import ConfigParser
import pyspark.sql.functions as f
from pyspark.sql import SparkSession


class Gdelt_Transformer(object):
    """
    read movie match-up table, perform .filter(), .join(), .split() tasks
    :return df with matched links and split tone score metrics
    """

    def __init__(self, month, year, tone, df):
        self.month = month
        self.year = year
        self.tone = tone
        self.df = df

    def match(self):
        """
        filter movie data by year, and filter GDELT data by year and month
        for each month, match the links by movie title and director/cast name, save to .parquet on S3
        """
        movie_df = self.df.filter(df.release_year == self.year)
        bucket = config.get('AWS', 'bucket')
        g_df = spark.read \
            .option("delimiter", "\t").csv("s3a://{}/gdelt/".format(bucket) + self.year + self.month + '*.gkg.csv',
                                           header=True)
        cond = [g_df.SOURCEURLS.contains(movie_df.movie_title),
                (g_df.PERSONS.contains(df.cast)) | (g_df.PERSONS.contains(movie_df.director))]
        tmp = movie_df.join(g_df, on=cond)
        tmp = tmp.groupBy(tmp.show_id, tmp.movie_title, tmp.SOURCEURLS, tmp.NUMARTS, tmp.TONE).count().withColumn(
            "MONTH", f.lit(self.month)).withColumn("YEAR", f.lit(self.year))
        tmp = self.col_split(tmp)
        tmp.write.parquet("s3a://{}/parquet/".format(bucket) + self.year + self.month + ".parquet")

    def col_split(self, tmp):
        """
        split tone score metrics for future analysis
        """
        split_col = f.split(f.col(self.tone), ",")
        tone_col = ["tone_score", "positive_score", "negative_score", "polarity", "act_ref_den", "self_group_den"]
        tmp = tmp.withColumn(tone_col[0], split_col.getItem(0)) \
            .withColumn(tone_col[1], split_col.getItem(1)) \
            .withColumn(tone_col[2], split_col.getItem(2)) \
            .withColumn(tone_col[3], split_col.getItem(3)) \
            .withColumn(tone_col[4], split_col.getItem(4)) \
            .withColumn(tone_col[5], split_col.getItem(5))
        return tmp


class ReadParquet(object):

    def __init__(self, folder_name, file_name):
        self.folder_name = folder_name
        self.file_name = file_name
        self.bucket = config.get('AWS', 'bucket')
        self.path = 's3a://{}/{}/{}'.format(self.bucket, self.folder_name, self.file_name)

    def load(self):
        print(self.path)
        parquet = spark.read.parquet(self.path)
        return parquet


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
    file_name = "movie_lookup.parquet"
    parquet_task = ReadParquet(folder_name, file_name)
    df = parquet_task.load()

    for n in range(2013, 2021):
        for m in range(1, 13):
            year = str(n).zfill(4)
            month = str(m).zfill(2)
            df = Gdelt_Transformer(month, year, "TONE", df)
            try:
                df.match()
            except:
                continue
