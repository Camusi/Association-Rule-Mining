import dask.dataframe as dd
from dask import bag as db
import matplotlib.pyplot as plt
import numpy as np


# Set up ---------------------------------------------------------------------------------------------------------------

def load_data(num_users, num_movies):
    movies_dd = dd.read_csv("movies.csv")
    ratings_dd = dd.read_csv("ratings.csv")

    top_movies = movies_dd["movieId"].drop_duplicates().compute().values[:num_movies]
    top_movies_set = set(top_movies)

    filt_ratings = ratings_dd[ratings_dd["movieId"].isin(top_movies_set)]

    unique_users = filt_ratings["userId"].drop_duplicates().compute().values[:num_users]
    unique_users_set = set(unique_users)
    filt_ratings = filt_ratings[filt_ratings["userId"].isin(unique_users_set)].drop_duplicates(subset=["userId", "movieId"])

    filt_ratings_pd = filt_ratings.compute()

    # Index mapping
    sorted_users = np.sort(unique_users)
    sorted_movies = np.sort(top_movies)
    user_indices = np.searchsorted(sorted_users, filt_ratings_pd["userId"].values)
    movie_indices = np.searchsorted(sorted_movies, filt_ratings_pd["movieId"].values)

    # Creates binary watch arrays for each user
    watch_matrix = np.zeros((len(sorted_users), len(sorted_movies)), dtype=int)
    watch_matrix[user_indices, movie_indices] = 1

    user_show_list = list(zip(sorted_users, watch_matrix))
    user_show_ratings_bag = db.from_sequence(user_show_list)

    movies_dict = movies_dd.compute().set_index("movieId")["title"].to_dict()
    show_titles = [movies_dict[m] for m in sorted_movies]
    show_names_bag = db.from_sequence(list(enumerate(show_titles)))

    return user_show_ratings_bag, show_names_bag

# ----------------------------------------------------------------------------------------------------------------------


# A-priori Algorithm ---------------------------------------------------------------------------------------------------

def generate_frequent_pairs(watched_indices, above_threshold):
    # Above_threshold is the list of show indices that individually had supports > 100
    def generate_pairs(show_indices):
        pairs = []
        show_indices = list(show_indices)   # The indices of shows that were watched by a viewer e.g. [1, 3, 5]
        for i in range(len(show_indices)):
            for j in range(i + 1, len(show_indices)):
                if show_indices[i] in above_threshold and show_indices[j] in above_threshold:
                    pairs.append(tuple(sorted((show_indices[i], show_indices[j]))))
        return pairs

    pair_bag = watched_indices.map(generate_pairs).flatten()
    return pair_bag.frequencies()


def a_priori(user_show_bag, support=100):
    # Step 1: Extract watched show indices (indices where value = 1), then flatten the output
    watched_indices = user_show_bag.map(lambda x: [index for index, val in enumerate(x[1]) if val == 1])
    flattened = watched_indices.flatten()

    # Step 2: Get shows watched individually over 100 (threshold) times
    show_frequencies = flattened.frequencies()
    shows_above_threshold = show_frequencies.filter(lambda x: x[1] >= support).map(lambda x: x[0]).compute()

    # Step 3: Get frequent pairs
    pair_counts = generate_frequent_pairs(watched_indices, set(shows_above_threshold))
    return pair_counts

# ----------------------------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    user_show_ratings_bag, show_names_bag = load_data(330975, 100)

    # ----- Get frequent item pairs ------------------------------------------------------------------------------------
    frequent_pairs = a_priori(user_show_ratings_bag, support=100)
    print(frequent_pairs.take(10))
    # ------------------------------------------------------------------------------------------------------------------


    # ----- Convert frequent item pair numbers to actual movie names ---------------------------------------------------
    show_names_list = show_names_bag.map(lambda x: x[1]).compute()  # Just get show names

    frequent_pairs_list = frequent_pairs.compute()

    named_pairs = []
    for (i, j), count in frequent_pairs_list:
        name_i = show_names_list[i]
        name_j = show_names_list[j]
        named_pairs.append(((name_i, name_j), count))

    # Show top 10
    for (show_a, show_b), count in named_pairs[:10]:
        print(f"{show_a} & {show_b} — watched together by {count} users")
    # ------------------------------------------------------------------------------------------------------------------

    # ----- Association rules ------------------------------------------------------------------------------------------
    def generate_rules(pair_counts, item_supports, min_confidence=0.7):
        rules = []
        for (a, b), support_ab in pair_counts:
            support_a = item_supports.get(a, 0)
            support_b = item_supports.get(b, 0)
            if support_a > 0:   # # If they watched a then we have x confidence they have watched b (a -> b)
                conf_a_to_b = support_ab / support_a
                if conf_a_to_b >= min_confidence:
                    rules.append(((a, b), conf_a_to_b, support_ab))
            if support_b > 0:   # If they watched b then we have x confidence they have watched a (b -> a)
                conf_b_to_a = support_ab / support_b
                if conf_b_to_a >= min_confidence:
                    rules.append(((b, a), conf_b_to_a, support_ab))
        return rules


    # Get individual supports
    item_supports = user_show_ratings_bag.map(lambda x: set(x[1].nonzero()[0])).flatten().frequencies().compute()

    # Create rules
    rules = generate_rules(frequent_pairs, dict(item_supports), min_confidence=0.7)
    named_rules = [((show_names_list[a], show_names_list[b]), conf, support) for ((a, b), conf, support) in rules]

    # Print top 10
    for (a, b), conf, support in named_rules[:10]:
        print(f"If a user watches '{a}', they also watch '{b}' with confidence {conf:.2f} (support={support})")
    # ------------------------------------------------------------------------------------------------------------------


    # ----- Graphing the data ------------------------------------------------------------------------------------------
    top_rules = sorted(named_rules, key=lambda x: x[1], reverse=True)[:20]

    labels = [f"{a} → {b}" for (a, b), _, _ in top_rules]
    confidences = [conf for (_, _), conf, _ in top_rules]
    supports = [support for (_, _), _, support in top_rules]

    # Make confidence and support graphs display next to each other
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(16, 8), sharey=True)

    # Confidence graph
    axes[0].barh(labels[::-1], confidences[::-1], color='skyblue')
    axes[0].set_title("Confidence")
    axes[0].set_xlabel("Confidence")
    axes[0].set_xlim(0.7, 1.0)

    # Support graph
    axes[1].barh(labels[::-1], supports[::-1], color='salmon')
    axes[1].set_title("Support")
    axes[1].set_xlabel("Support Count")

    plt.tight_layout()
    plt.show()
    # ------------------------------------------------------------------------------------------------------------------
