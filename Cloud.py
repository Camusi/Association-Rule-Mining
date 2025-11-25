%%writefile cowatched_film_rules.py
import dask.dataframe as dd
import pandas as pd
from dask import bag as db
import time
import multiprocessing
import sys
import numpy as np
from dask.distributed import get_task_stream
import os

def load_data(num_users, num_movies):
    movies_df = dd.read_csv("movies.csv")       # Columns: movieId,title,genres
    ratings_df = dd.read_csv("ratings.csv")     # Columns: userId,movieId,rating,timestamp

    top_x_movies = movies_df.head(num_movies)["movieId"].tolist()
    filtered_ratings = ratings_df[ratings_df["movieId"].isin(top_x_movies)]

    print(f"Getting {num_users} users...")
    unique_users = filtered_ratings["userId"].drop_duplicates().compute().head(num_users).tolist()
    limited_ratings = filtered_ratings[filtered_ratings["userId"].isin(unique_users)]

    limited_ratings = limited_ratings[["userId", "movieId"]].drop_duplicates()
    limited_ratings["watched"] = 1

    limited_ratings["movieId"] = limited_ratings["movieId"].astype(pd.CategoricalDtype(categories=top_x_movies))

    pivot_df = limited_ratings.pivot_table(index="userId", columns="movieId", values="watched", aggfunc="sum").fillna(0).astype(int)

    pivot_df = pivot_df[sorted(top_x_movies)]

    print("Generating result preview...")
    result_df = pivot_df.compute()
    print(result_df.head())

    pivot_pd = pivot_df.compute()

    user_show_list = [(user, row.values) for user, row in pivot_pd.iterrows()]
    user_show_ratings_bag = db.from_sequence(user_show_list)

    movies_pd = movies_df.compute()

    show_names_list = movies_pd[movies_pd["movieId"].isin(top_x_movies)].set_index("movieId").loc[top_x_movies]["title"].tolist()
    show_names_bag = db.from_sequence(list(enumerate(show_names_list)))

    return user_show_ratings_bag, show_names_bag


def generate_frequent_pairs(watched_indices, above_threshold):
    above_threshold = set(above_threshold)

    def generate_pairs(show_indices):
        filtered = list(filter(lambda x: x in above_threshold, show_indices))
        pairs = list(map(lambda ij: tuple(sorted((filtered[ij[0]], filtered[ij[1]]))),
            ((i, j) for i in range(len(filtered)) for j in range(i + 1, len(filtered)))))
        return pairs

    return watched_indices.map(generate_pairs).flatten().frequencies()

def a_priori(user_show_bag, support=100):
    watched_indices = user_show_bag.map(lambda x: [index for index, val in enumerate(x[1]) if val == 1])
    flattened_watched_indices = watched_indices.flatten()
    show_frequencies = flattened_watched_indices.frequencies()
    shows_above_threshold = show_frequencies.filter(lambda x: x[1] >= support).map(lambda x: x[0]).compute()
    pair_counts = generate_frequent_pairs(watched_indices, set(shows_above_threshold))
    return pair_counts

def generate_rules(pair_counts, item_supports, min_confidence=0.7):
    rules = []
    for (a, b), support_ab in pair_counts:
        support_a = item_supports.get(a, 0)
        support_b = item_supports.get(b, 0)
        if support_a > 0:
            conf_a_to_b = support_ab / support_a
            if conf_a_to_b >= min_confidence:
                rules.append(((a, b), conf_a_to_b, support_ab))
        if support_b > 0:
            conf_b_to_a = support_ab / support_b
            if conf_b_to_a >= min_confidence:
                rules.append(((b, a), conf_b_to_a, support_ab))
    return rules

def cowatched_film_rules(n_users=10000, support_threshold=100, show_file='shows.txt', user_file='user-shows.txt'):
    # Step 1: Load data
    user_show_ratings_bag, show_names_bag = load_data(n_users, 100)
    show_names_list = show_names_bag.compute()

    # Step 2: Run full algorithm
    time_start = time.time()
    frequent_pairs = a_priori(user_show_ratings_bag, support_threshold)
    frequent_pairs_list = frequent_pairs.compute()

    item_supports = user_show_ratings_bag.map(lambda x: set(np.nonzero(x[1])[0])).flatten().frequencies().compute()
    rules = generate_rules(frequent_pairs_list, dict(item_supports), min_confidence=0.7)

    named_rules = [((show_names_list[a], show_names_list[b]), conf, support)
                   for ((a, b), conf, support) in rules]
    time_end = time.time()

    # Step 3: Output
    print("Top 3 rules:")
    for (a, b), conf, support in named_rules[:3]:
        print(f"If a user watches '{a}', they also watch '{b}' with confidence {conf:.2f} (support={support})")

    runtime = time_end - time_start
    print(f"\nProcessed {n_users} users in {runtime:.2f} seconds")
    return runtime

if __name__ == "__main__":
    from dask.distributed import LocalCluster, Client

    # Download files once
    if not os.path.exists("movie_data.zip"):
      os.system("pip install -q gdown")
      os.system("gdown -O lab4_part1.zip 'https://drive.google.com/uc?export=download&id=11s0A5FwzYlwoy0zwSBhk74q5Db9y4_6C'")
      os.system("unzip -o lab4_part1.zip")
    print("Files downloaded")

    show_file = 'shows.txt'
    user_file = 'user-shows.txt'

    # Parse command-line arguments
    support_threshold = 100
    n_workers = multiprocessing.cpu_count()

    if len(sys.argv) >= 2:
        n_users = int(sys.argv[1])
    if len(sys.argv) >= 3:
        support_threshold = int(sys.argv[2])
    if len(sys.argv) >= 4:
        n_workers = int(sys.argv[3])

    # Start cluster and client
    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=1)
    print(f"Running with {n_workers} workers")
    client = Client(cluster)

    # Optional: Profile the full execution
    with get_task_stream(plot='save', filename=f"task-stream-{n_workers}.html"):
        cowatched_film_rules(n_users=n_users, support_threshold=support_threshold,
                             show_file=show_file, user_file=user_file)

    client.close()
    cluster.close()