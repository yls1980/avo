import psycopg2
import networkx as nx
import matplotlib.pyplot as plt
import secr

conn = psycopg2.connect(
    host="10.122.3.134",
    port="5432",
    database="greenplum-dwh",
    user="gpadmin",
    password=secr.pss()
)

with conn.cursor() as cur:
    cur.execute("""
        SELECT schema1_name, table1_name, column1_name, schema2_name, table2_name, column2_name
        FROM public.table_relationships
        WHERE flag = TRUE;
    """)
    relationships = cur.fetchall()

conn.close()

G = nx.DiGraph()

for schema1, table1, column1, schema2, table2, column2 in relationships:
    source = f"{schema1}.{table1}.{column1}"
    target = f"{schema2}.{table2}.{column2}"
    G.add_edge(source, target)

plt.figure(figsize=(12, 8))
pos = nx.spring_layout(G, seed=42)  # For consistent layout
nx.draw(G, pos, with_labels=True, node_size=5000, node_color="lightblue", font_size=8, font_weight="bold", edge_color="gray")
plt.title("Database Table Lineage Based on Relationships")
plt.show()
