Installation
============

Requirements
------------

* Python 3.9 or higher
* pip package manager

Install from PyPI
-----------------

The easiest way to install the Antfly SDK is using pip:

.. code-block:: bash

   pip install antfly-sdk

Install from Source
-------------------

To install from source, clone the repository and install in development mode:

.. code-block:: bash

   git clone https://github.com/yourusername/antfly-sdk.git
   cd antfly-sdk
   pip install -e .

Development Installation
------------------------

For development, install with additional dependencies:

.. code-block:: bash

   pip install -e .[dev]

This will install:

* pytest for testing
* black and ruff for code formatting and linting
* mypy for type checking
* sphinx for documentation

Generating the Client
---------------------

The SDK uses an auto-generated client based on the Antfly OpenAPI specification. To regenerate the client:

.. code-block:: bash

   make generate

This requires the ``openapi-python-client`` package to be installed.

Verifying Installation
----------------------

To verify the installation:

.. code-block:: python

   import antfly
   print(antfly.__version__)
