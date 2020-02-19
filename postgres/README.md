#### Tables Names
df: joined table with GDELT and Netflix

newdb_movie: movie look-up table

Both tables are stored in PostgreSQL database.
#### Tables Schema
Table Name: df
```
 |-- show_id: text (nullable = true)
 |-- movie_title: text (nullable = true)
 |-- source_url: text (nullable = true)
 |-- num_of_arts: bigint (nullable = true)
 |-- scoring_metrics: text (nullable = true)
 |-- count: bigint (nullable = true)
 |-- publish_month: bigint (nullable = true)
 |-- publish_year: bigint (nullable = true)
 |-- tone_score: numeric (nullable = true)
 |-- positive_score: numeric (nullable = true)
 |-- negative_score: numeric (nullable = true)
 |-- polarity: numeric (nullable = true)
 |-- act_ref_den: numeric (nullable = true)
 |-- self_group_den: numeric (nullable = true)
```

Table Name: newdb_movie
```
 |-- show_id: bigint (nullable = true)
 |-- type: text (nullable = true)
 |-- title: text (nullable = true)
 |-- director: text (nullable = true)
 |-- cast: text (nullable = true)
 |-- country: text (nullable = true)
 |-- date_added: bigint (nullable = true)
 |-- release_year: bigint (nullable = true)
 |-- rating: numeric (nullable = true)
 |-- duration: text (nullable = true)
 |-- listed_in: numeric (nullable = true)
 |-- description: text (nullable = true)
 |-- movie_title: text (nullable = true)
```
#### Get Started
Connect Tableau server to PostgreSQL database, apply "Custom SQL Query" by `./postgres_query.sql` and right join the two tables. 

![](../images/tableau_data_source.png)
