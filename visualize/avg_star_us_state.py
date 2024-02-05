import plotly.express as px
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.execute('use "Yelp"')
with open('../Queries/select_avg_stars_by_state.cql') as query_file:
    query = query_file.read()
    rows = session.execute(query)
    rows = list(rows)
    state = [row.state for row in rows]
    avg_star = [row.avg_star for row in rows]

fig = px.choropleth(locations=state, locationmode="USA-states", color=avg_star, scope="usa", range_color=(0, 5))
fig.add_scattergeo(
    locations=state,    ###codes for states,
    locationmode='USA-states',
    text=state,
    mode='text')
fig.show()
