.. antfly-sdk documentation master file

Welcome to Antfly SDK Documentation
====================================

The Antfly SDK is a Python client library for interacting with the Antfly database system - a distributed key-value store with powerful search capabilities.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   quickstart
   api_reference
   examples

Features
--------

* **Simple, intuitive API** for database operations
* **Support for authenticated and unauthenticated** connections
* **Full-text and semantic search** capabilities
* **Batch operations** for efficient data manipulation
* **Type-safe operations** with Python type hints
* **Comprehensive error handling** with specific exception types

Quick Example
-------------

.. code-block:: python

   from antfly import AntflyClient

   # Create a client
   client = AntflyClient(
       base_url="http://localhost:8080",
       username="admin",
       password="password"
   )

   # Create a table
   table = client.create_table(
       name="users",
       num_shards=2
   )

   # Insert data
   client.batch(
       table="users",
       inserts={
           "user:1": {"name": "Alice", "age": 30},
           "user:2": {"name": "Bob", "age": 25}
       }
   )

   # Query data
   results = client.query(
       table="users",
       full_text_search={"query": "Alice"}
   )

   # Get a specific record
   user = client.get(table="users", key="user:1")
   print(user["name"])  # Output: Alice

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

