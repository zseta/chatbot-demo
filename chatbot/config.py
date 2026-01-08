import os

SCYLLADB_CONFIG = {
    "host": os.getenv("SCYLLADB_HOST", ""),
    "port": os.getenv("SCYLLADB_PORT", "9042"),
    "username": os.getenv("SCYLLADB_USERNAME", "scylla"),
    "password": os.getenv("SCYLLADB_PASSWORD", ""),
    "datacenter": os.getenv("SCYLLADB_DATACENTER", "AWS_US_EAST_1"),
    "keyspace": os.getenv("SCYLLADB_KEYSPACE", "recommend")
}

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
