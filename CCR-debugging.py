# Databricks notebook source
# MAGIC %md
# MAGIC # how to debug code
# MAGIC
# MAGIC Writing code is often the easy part. It tends to get difficult if things don't go as expected. Because of this, debugging is a crucial skill. This section will explore common error messages and other methods to capture and log errors.
# MAGIC
# MAGIC useful links for more extensive information:
# MAGIC - [summary python debugging notebook](https://www.freecodecamp.org/news/python-debugging-handbook/#heading-some-additional-tips-for-efficient-debugging)
# MAGIC - [python logging](https://realpython.com/python-logging/)
# MAGIC - [raising exceptions in python](https://realpython.com/python-raise-exception/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Printing
# MAGIC print, print, print. It's often the easiest way to initially get a glimpse of the flow of your functions or code. Inspect variables and see what they entail and whether they contain the information you expect the to. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logging
# MAGIC Logging is like writing notes while your functions run. But instead of printing things to the screen, you write them to a log. It helps to keep track of things expecially as they might be going wrong in production. Logs typically have the following levels:
# MAGIC
# MAGIC - `debug`- information typically only of interest when debugging
# MAGIC - `info`- comfirmation that things work as expected
# MAGIC - `warning`- indication that something unexpected happened
# MAGIC - `error`- a more serious problem that prevented the program from performing what was expected
# MAGIC
# MAGIC Within amif we use it extensively to log information in our workflows

# COMMAND ----------

import logging
logging.debug("This is a debug message")

logging.info("This is an info message")

logging.warning("This is a warning message")

logging.error("This is an error message")

logging.critical("This is a critical message")

# debug and info did not get logged, that is because by default the logging module logs the messages with a severity level of WARNING or above. 

# COMMAND ----------

# MAGIC %md
# MAGIC # Try-except
# MAGIC
# MAGIC Try- exept blocks are being used to handle exceptions that might occur during the execiutoion of code. It allows to capture errors so that the program won't crash unexpectatedly.
# MAGIC
# MAGIC

# COMMAND ----------

# this would usually create an error
10/0

# COMMAND ----------

# try Contains the code that you want to attempt executing. If an exception occurs in this block, Python will jump to the except block.
try:
    # Try to divide by zero (will raise an exception)
    result = 10 / 0

# except block: Defines what to do when an error (exception) occurs in the try block.
except ZeroDivisionError as e:
    # Handle the specific exception (ZeroDivisionError)
    print(f"Error: {e}")

# COMMAND ----------

# however be careful when being to general in capturing errors, it could obscure the root cause of a problem making debugging difficult, hiding away critical errors
try:
    # Some code
except Exception as e:
    print(f"An error occurred: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC # Common Python Errors and Debugging Examples

# COMMAND ----------

# MAGIC %md
# MAGIC ## SyntaxError
# MAGIC Occurs when the code structure is invalid. It could be a missing parenthesis, a misplaced colon, or some other syntax-related issue. To fix these types of errors, check for missing syntax elements and ensure proper pairing of quotes, parentheses, and brackets.

# COMMAND ----------

if True print("Hello!")

# COMMAND ----------

print("Hello!"

# COMMAND ----------

# MAGIC %md
# MAGIC ## IndentationError
# MAGIC Occurs when indentation is inconsistent or missing.

# COMMAND ----------

def greet():
print("Hello!") 

# COMMAND ----------

# MAGIC %md
# MAGIC ## TypeError
# MAGIC Occurs when an operation is applied to an incompatible data type.

# COMMAND ----------

# incompatible types e.g. adding string and integer
result = "hello" + 5 

# COMMAND ----------

# calling a function with incorrect number of arguments

def multiply(a, b):
    return a * b

multiply(10, 20, 30)

# COMMAND ----------

# calling an object like a function that is not callable
variable = "hello"
variable()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ValueError
# MAGIC Occurs when an operation receives an argument of the right type but inappropriate value.

# COMMAND ----------

# Error: invalid literal for int() with base X
# Cause: String contains non-numeric characters
# Fix: Ensure valid numeric input
float("10a")

# COMMAND ----------

# Error: could not convert string to float
# Cause: String is not a valid float
# Fix: Check if input is numeric
float("abc")

# COMMAND ----------

# Error: invalid literal for int() with base 10
# Cause: invalid literal for int() with base 10
# Fix: Convert to float() first, then int()
int("12.34")

# COMMAND ----------

# Error: unhashable type: 'mutable_type'
# Cause: Using a mutable type as a dictionary key
# Fix: Use immutable types (e.g., tuple)
test_dict = {{1, 2, 3}: "set as a key"}


# COMMAND ----------

# MAGIC %md
# MAGIC ## NameError
# MAGIC Occurs when a variable is used before it is defined. Make sure to check for typos in variable or function names, and make sure they are defined before use.

# COMMAND ----------

print(message) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## IndexError
# MAGIC Occurs when trying to access an element outside the valid index range of a list/string or tuple. To fix it, make sure that the index being used is within the valid range of the sequence.

# COMMAND ----------

my_list = [1, 2, 3]
print(my_list[5])

# COMMAND ----------

# MAGIC %md
# MAGIC ## KeyError
# MAGIC Occurs when trying to access a key that does not exist in a dictionary.

# COMMAND ----------

person = {"name": "Alice", "age": 30}
print(person["city"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## ZeroDivisionError
# MAGIC Occurs when dividing by zero.

# COMMAND ----------

result = 10 / 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## AttributeError: Missing Attribute
# MAGIC Occurs when trying to access an attribute or method that doesn’t exist on an object. To fix this, review the code and confirm that the attribute or method being called is correct and available.
# MAGIC

# COMMAND ----------

my_list = [1, 2, 3]
my_list.upper() 

# COMMAND ----------

# MAGIC %md
# MAGIC ## ImportError / ModuleNotFoundError
# MAGIC Occurs when a module cannot be found or imported. To avoid this, install the required module using a package manager (pip) or check the module name for typos.

# COMMAND ----------

import nonexistent_module

# COMMAND ----------

# MAGIC %md
# MAGIC ## FileNotFoundError
# MAGIC Occurs when trying to access a file that does not exist. You should check the file path and make sure the file exists at the specified location.

# COMMAND ----------

with open("nonexistent_file.txt", "r") as file:
    content = file.read()

# COMMAND ----------

# MAGIC %md
# MAGIC ## TypeError: 'NoneType' Object is Not Callable
# MAGIC Occurs when trying to call a None object as if it were a function.

# COMMAND ----------

my_var = None
my_var()

# COMMAND ----------

# MAGIC %md
# MAGIC # Common pyspark errors
# MAGIC
# MAGIC ##### TODO
# MAGIC
# MAGIC Links:
# MAGIC - [more generic databricks errors](https://learn.microsoft.com/en-us/azure/databricks/error-messages/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercises
# MAGIC Try fixing the following buggy functions:

# COMMAND ----------


def multiply(a, b)
return a * b

# COMMAND ----------

print(greeting)

# COMMAND ----------

def divide_numbers(a, b):
    return a / b

result = divide_numbers(10, "5")

# COMMAND ----------

colors = ["red", "blue", "green"]
print(colors[3])

# COMMAND ----------

user = {"username": "jdoe", "email": "jdoe@example.com"}
print(user["age"])