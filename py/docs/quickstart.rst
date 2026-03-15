Quick Start Guide
=================

This guide will help you get started with the Antfly SDK.

Creating a Client
-----------------

First, import and create an Antfly client:

.. code-block:: python

   from antfly import AntflyClient

   # Create an unauthenticated client
   client = AntflyClient(base_url="http://localhost:8080")

   # Or create an authenticated client
   client = AntflyClient(
       base_url="http://localhost:8080",
       username="admin",
       password="password"
   )

Working with Tables
-------------------

Creating a Table
~~~~~~~~~~~~~~~~

.. code-block:: python

   table = client.create_table(
       name="products",
       num_shards=4,
       schema={
           "properties": {
               "name": {"type": "string"},
               "price": {"type": "number"},
               "description": {"type": "string"}
           }
       }
   )

Listing Tables
~~~~~~~~~~~~~~

.. code-block:: python

   tables = client.list_tables()
   for table in tables:
       print(f"Table: {table.name}")

Getting Table Details
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   table_status = client.get_table("products")
   print(f"Shards: {table_status.num_shards}")
   print(f"Document count: {table_status.doc_count}")

Dropping a Table
~~~~~~~~~~~~~~~~

.. code-block:: python

   client.drop_table("products")

Working with Data
-----------------

Inserting Records
~~~~~~~~~~~~~~~~~

Use batch operations to insert one or more records:

.. code-block:: python

   client.batch(
       table="products",
       inserts={
           "prod:1": {
               "name": "Laptop",
               "price": 999.99,
               "description": "High-performance laptop"
           },
           "prod:2": {
               "name": "Mouse",
               "price": 29.99,
               "description": "Wireless mouse"
           }
       }
   )

Getting a Record
~~~~~~~~~~~~~~~~

.. code-block:: python

   product = client.get(table="products", key="prod:1")
   print(product["name"])  # Output: Laptop

Deleting Records
~~~~~~~~~~~~~~~~

.. code-block:: python

   client.batch(
       table="products",
       deletes=["prod:1", "prod:2"]
   )

Querying Data
-------------

Full-Text Search
~~~~~~~~~~~~~~~~

.. code-block:: python

   results = client.query(
       table="products",
       full_text_search={
           "query": "laptop",
           "fields": ["name", "description"]
       },
       limit=10
   )

   for hit in results.hits:
       print(f"Found: {hit.id} - Score: {hit.score}")

Semantic Search
~~~~~~~~~~~~~~~

.. code-block:: python

   results = client.query(
       table="products",
       semantic_search="high-quality computer peripherals",
       limit=5
   )

Prefix Filtering
~~~~~~~~~~~~~~~~

.. code-block:: python

   # Get all products with keys starting with "prod:laptop:"
   results = client.query(
       table="products",
       filter_prefix="prod:laptop:",
       limit=100
   )

Error Handling
--------------

The SDK provides specific exception types for different error scenarios:

.. code-block:: python

   from antfly import AntflyException, AntflyConnectionError, AntflyAuthError

   try:
       client.get(table="products", key="nonexistent")
   except AntflyException as e:
       print(f"Operation failed: {e}")
