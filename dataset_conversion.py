import dask.dataframe as dd
import pandas as pd

print("Reading...")
movies_df = dd.read_csv("movies.csv")       # Columns: movieId,title,genres
ratings_df = dd.read_csv("ratings.csv")     # Columns: userId,movieId,rating,timestamp

print("Getting first 20 movies...")
top_20_movies = movies_df.head(20)["movieId"].tolist()

print("Filtering ratings.csv to only include those 20 movies...")
filtered_ratings = ratings_df[ratings_df["movieId"].isin(top_20_movies)]

print("Getting 1000 users...")
unique_users = filtered_ratings["userId"].drop_duplicates().compute().head(1000).tolist()
limited_ratings = filtered_ratings[filtered_ratings["userId"].isin(unique_users)]

print("Preparing binary watched column...")
limited_ratings = limited_ratings[["userId", "movieId"]].drop_duplicates()
limited_ratings["watched"] = 1

limited_ratings["movieId"] = limited_ratings["movieId"].astype(
    pd.CategoricalDtype(categories=top_20_movies)
)

print("Pivoting to binary matrix (dask DataFrame)...")
pivot_df = limited_ratings.pivot_table(
    index="userId",
    columns="movieId",
    values="watched",
    aggfunc="sum"   # allowed aggregation
).fillna(0).astype(int)

print("Sorting columns for consistency...")
pivot_df = pivot_df[sorted(top_20_movies)]

print("Result preview (compute to pandas):")
result_df = pivot_df.compute()
print(result_df.head())
