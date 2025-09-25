# neo4j_setup.py
import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER" )
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

INDEX_QUERIES = [
    # Fulltext index for generic nodes
    """
    CREATE FULLTEXT INDEX node_name_and_summary
    IF NOT EXISTS
    FOR (n:Node) ON EACH [n.name, n.summary]
    """,

    # Fulltext index for Episodic nodes
    """
    CREATE FULLTEXT INDEX episodic_content
    IF NOT EXISTS
    FOR (e:Episodic) ON EACH [e.content, e.name, e.summary]
    """,

    # Optional: index for relationships if Graphiti needs them
    """
    CREATE FULLTEXT INDEX relationship_text
    IF NOT EXISTS
    FOR ()-[r:RELATED_TO]-() ON EACH [r.description]
    """,

    """
    CREATE FULLTEXT INDEX node_name_and_summary
    IF NOT EXISTS
    FOR (n:Entity)
    ON EACH [n.name, n.summary]
    """,

    """
    CREATE FULLTEXT INDEX edge_name_and_fact
    IF NOT EXISTS
    FOR ()-[r:RELATES_TO]-()
    ON EACH [r.name, r.fact]
    """
]


def setup_indexes():
    with driver.session() as session:
        for query in INDEX_QUERIES:
            try:
                session.run(query)
                print(f"✅ Executed: {query.strip().splitlines()[0]} ...")
            except Exception as e:
                print(f"⚠️  Failed on query: {query}\n   {e}")

def seed_test_data():
    with driver.session() as session:
        session.run("""
        MERGE (n:Node {uuid: "test-node"})
        SET n.name = "Test Node", n.summary = "This is a seeded test node"
        """)
        session.run("""
        MERGE (e:Episodic {uuid: "test-episode"})
        SET e.name = "Test Episode",
            e.content = "Alice works at Acme Corp since 2021.",
            e.summary = "Employment test",
            e.created_at = datetime()
        """)
        print("✅ Seeded test node + episode")

if __name__ == "__main__":
    print("🚀 Setting up Neo4j schema for Graphiti...")
    setup_indexes()
    seed_test_data()
    print("🎉 Setup complete.")
    driver.close()
