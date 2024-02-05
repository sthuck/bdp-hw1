import pandas as pd
from cassandra.cluster import Cluster
import matplotlib.pyplot as plt

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.execute('use "Yelp"')
with open('../Queries/select_avg_stars_by_user_signup.cql') as query_file:
    query = query_file.read()
    rows = session.execute(query)
    rows = list(rows)
    data = [row.system_avg_average_stars for row in rows]
    index = [str(row.yelping_since_year) + "-" + str(row.yelping_since_month) for row in rows]

    df = pd.DataFrame(data, index=index, columns=['avg_star'])
    df.sort_index(inplace=True)
    df.plot(xlabel="signup date", ylabel="average stars", title="Average stars by user signup date")
    plt.show()
    print(df)

