SELECT df.movie_title, SUM(df.num_of_arts), newdb_movie.release_year, newdb_movie.country, newdb_movie.type, newdb_movie.title, newdb_movie.description, newdb_movie.listed_in
FROM df
JOIN newdb_movie ON df.movie_title=newdb_movie.movie_title
WHERE newdb_movie.release_year IN ('2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020')
GROUP BY df.movie_title, newdb_movie.release_year, newdb_movie.country, newdb_movie.type, newdb_movie.title, newdb_movie.description, newdb_movie.listed_in
ORDER BY SUM(df.num_of_arts) DESC
