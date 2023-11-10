import mysql.connector
import json

config_file = 'connectorConfig.json'
with open(config_file, "r") as f:
    config = json.load(f)

connection_config = config["mysql"]

def release_year_count():
    # Connect to the MySQL database
    data_base = mysql.connector.connect(**connection_config)
    cursor = data_base.cursor()

    # SQL query to count the number of series released in each year
    query = """
        SELECT ReleaseYear, COUNT(*) AS count
        FROM tvseries
        GROUP BY ReleaseYear
        ORDER BY ReleaseYear
    """

    # Execute the query
    cursor.execute(query)

    # Fetch all the results as a list of tuples
    result = cursor.fetchall()

    # Close the database connection
    data_base.close()

    return result


def get_series_by_star(star_name):
    # Connect to the MySQL database
    data_base = mysql.connector.connect(**connection_config)
    cursor = data_base.cursor()

    # SQL query to get series titles for a given star
    query = f"""
        SELECT B.title
        FROM seriesstars A
        JOIN tvseries B ON A.IMDB_id = B.IMDB_id
        WHERE A.star = "{star_name}";
    """

    # Execute the query
    cursor.execute(query)

    # Fetch all the results as a list of tuples
    result = cursor.fetchall()

    # Close the database connection
    data_base.close()

    return result


def get_series_by_star_and_genre(star_name, genre):
    # Connect to the MySQL database
    data_base = mysql.connector.connect(**connection_config)
    cursor = data_base.cursor()

    # SQL query to get series titles and ratings for a given star and genre
    # Could add an "order by" here
    query = f"""
        SELECT B.title, B.rating
        FROM seriesstars A
        JOIN tvseries B ON A.IMDB_id = B.IMDB_id
        JOIN seriesgenre C ON B.IMDB_id = C.IMDB_id
        WHERE A.star = "{star_name}"
        AND C.genre = "{genre}";
    """

    # Execute the query
    cursor.execute(query)

    # Fetch all the results as a list of tuples
    result = cursor.fetchall()

    # Close the database connection
    data_base.close()

    return result


def get_series_costar(list_star_names):
    # Connect to the MySQL database
    data_base = mysql.connector.connect(**connection_config)
    cursor = data_base.cursor()

    # Build the JOIN and WHERE conditions for each star in the input list
    join_conditions = " ".join([f"JOIN seriesstars A{i} ON A{i-1}.IMDB_id = A{i}.IMDB_id" for i in range(1, len(list_star_names)+1)])
    where_conditions = " AND ".join([f"A{i}.star = '{star}'" for i, star in enumerate(list_star_names, start=1)])

    # SQL query to get series titles for the input list of stars
    query = f"""
        SELECT DISTINCT B.title
        FROM seriesstars A1
        {join_conditions}
        JOIN tvseries B ON A{len(list_star_names)}.IMDB_id = B.IMDB_id
        WHERE {where_conditions};
    """

    # Execute the query
    cursor.execute(query)

    # Fetch all the results as a list of tuples
    result = cursor.fetchall()

    # Close the database connection
    data_base.close()

    return result


def get_average_rating():
    # Connect to the MySQL database
    data_base = mysql.connector.connect(**connection_config)
    cursor = data_base.cursor()

    # SQL query to get the average rating
    query = """
        SELECT AVG(rating) FROM tvseries
    """

    # Execute the query
    cursor.execute(query)

    # Fetch the result
    average_rating = cursor.fetchone()[0]

    # Close the database connection
    data_base.close()

    return average_rating


def get_popular_series(star_name):
    # Get the average rating
    average_rating = get_average_rating()

    # Connect to the MySQL database
    data_base = mysql.connector.connect(**connection_config)
    cursor = data_base.cursor()

    # SQL query to get series titles for the input star with rating above the average rating
    query = f"""
        SELECT B.title
        FROM seriesstars A
        JOIN tvseries B ON A.IMDB_id = B.IMDB_id
        WHERE A.star = "{star_name}"
        AND B.rating > {average_rating};
    """

    # Execute the query
    cursor.execute(query)

    # Fetch all the results as a list of tuples
    result = cursor.fetchall()

    # Close the database connection
    data_base.close()

    return result


def get_rating_per_genre():
    # Connect to the MySQL database
    data_base = mysql.connector.connect(**connection_config)
    cursor = data_base.cursor()

    # SQL query to get the average rating for each genre
    query = """
        SELECT A.genre, AVG(B.rating) AS avg_rating
        FROM seriesgenre A
        JOIN tvseries B ON A.IMDB_id = B.IMDB_id
        GROUP BY A.genre
        ORDER BY avg_rating DESC;
    """

    # Execute the query
    cursor.execute(query)

    # Fetch all the results as a list of tuples
    result = cursor.fetchall()

    # Close the database connection
    data_base.close()

    return result


def get_series_director_star_genre(director, star, genre):
    # Connect to the MySQL database
    data_base = mysql.connector.connect(**connection_config)
    cursor = data_base.cursor()

    # SQL query to get series titles for the input director, star, and genre
    query = f"""
        SELECT T.title
        FROM tvseries T
        JOIN seriesdirector D ON T.IMDB_id = D.IMDB_id AND D.director = "{director}"
        JOIN seriesstars S ON T.IMDB_id = S.IMDB_id AND S.star = "{star}"
        JOIN seriesgenre G ON T.IMDB_id = G.IMDB_id AND G.genre = "{genre}";
    """

    # Execute the query
    cursor.execute(query)

    # Fetch all the results as a list of tuples
    result = cursor.fetchall()

    # Close the database connection
    data_base.close()

    return result


def main():
    release_year_list = release_year_count()
    star_series_list = get_series_by_star("Gabriel Luna")
    series_by_genre_list = get_series_by_star_and_genre("Gabriel Luna", "Crime")
    list_star_names = ['Arnold Schwarzenegger', 'Monica Barbaro', 'Gabriel Luna']
    # costars_list = get_series_costar(list_star_names)
    popular_list = get_popular_series("Arnold Schwarzenegger")
    genre_rating_list = get_rating_per_genre()
    specific_title_list = get_series_director_star_genre("Hidemaro Fujibayashi", "Kengo Takanashi", "Adventure")

    # Display the result
    # print(result_list) # This works! 1
    # print(star_series_list) # This works! 2
    # print(series_by_genre_list) # This works! 3
    # print(get_average_rating()) # This works! 5
    # print(popular_list) # This works! 5
    # print(genre_rating_list) # This works! 6
    # print(specific_title_list) # This works! 7

if __name__ == "__main__":
    main()