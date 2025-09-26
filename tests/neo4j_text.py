from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()


uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
user = os.environ.get("NEO4J_USER", "neo4j")
pwd = os.environ.get("NEO4J_PASSWORD", "your_password_here")

drv = GraphDatabase.driver(uri, auth=(user, pwd))
with drv.session() as sess:
    res = sess.run("RETURN 1 AS test")
    print(res.single()["test"])  # should print 1

drv.close()
