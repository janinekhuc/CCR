# Databricks notebook source
# MAGIC %md
# MAGIC # Python basics
# MAGIC
# MAGIC **Goal**: Cover basics for you to be able to better understand code and configurations used in python
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variables and data types
# MAGIC
# MAGIC A variable is a reserved memory location to store values. In Python, variables are created when you assign a value to them with `=` e.g. `random_variable = '123'`. In this case the variable called `random_variable` has a va;lue of `'123'` assigned to it.
# MAGIC
# MAGIC Python has several built-in data types that are used to store different kinds of data. Here are some of the most commonly used data types

# COMMAND ----------

# strings-----------------------------------
abc = '12345'
string_variable = "12345"
print(type(string_variable))

# integers/floats-----------------------------------
integer_variable = 12345
float_variable = 12345.0
print(type(integer_variable))
print(type(float_variable))

# booleans-----------------------------------
is_fun = True
print(type(is_fun))

# COMMAND ----------

# slices- python starts counting at 0-----------------------------------
print(string_variable[0])
print(string_variable[3])

# '12345' in string_variable
# '12' in string_variable

# COMMAND ----------

# strings usually don't take variables as input
value = 2.791514
value1= 1.36478
print(value)
print(value1)
'exact value is 2.791514'

# COMMAND ----------


# string formatting f'strings-----------------------------------
# variable can be added to a string and it's value will be added
# of what are the following variables?
value = 2.791514
print(f"exact value = '{value}'")
# print(f'approximate value = {value:.2f}')  # approximate value = 2.79
# print(f'string_variable = {string_variable}')
print(f'integer_variable = {integer_variable}')
print(f'float_variable = {float_variable}')
print(f'is_fun = {is_fun}')

# COMMAND ----------

# MAGIC %md
# MAGIC Here are some of the most common string methods. If the variable s is a string, then the code s.lower() runs the lower() method on that string object and returns the result (this idea of a method running on an object is one of the basic ideas that make up Object Oriented Programming, OOP). Here are some of the most common string methods:
# MAGIC
# MAGIC - s.lower(), s.upper() -- returns the lowercase or uppercase version of the string
# MAGIC - s.strip() -- returns a string with whitespace removed from the start and end
# MAGIC - s.isalpha()/s.isdigit()/s.isspace()... -- tests if all the string chars are in the various character classes
# MAGIC - s.startswith('other'), s.endswith('other') -- tests if the string starts or ends with the given other string
# MAGIC - s.find('other') -- searches for the given other string (not a regular expression) within s, and returns the first index where it begins or -1 if not found
# MAGIC - s.replace('old', 'new') -- returns a string where all occurrences of 'old' have been replaced by 'new'
# MAGIC - s.split('delim') -- returns a list of substrings separated by the given delimiter. The delimiter is not a regular expression, it's just text. 'aaa,bbb,ccc'.split(',') -> ['aaa', 'bbb', 'ccc']. As a convenient special case s.split() (with no arguments) splits on all whitespace chars.
# MAGIC - s.join(list) -- opposite of split(), joins the elements in the given list together using the string as the delimiter. e.g. '---'.join(['aaa', 'bbb', 'ccc']) -> aaa---bbb---ccc

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise

# COMMAND ----------

# A. Add two integers

# B. Add two strings (concatenate)

# C. multiply a float by an integer

# D. melkbroodjes
# Given an int count of a number of melkbroodjes, return a string
# of the form 'Number of melkbroodjes: <count>', where <count> is the number
# passed in. However, if the count is 10 or more, then use the word 'veel'
# instead of the actual count.
# So melkbroodjes(5) returns 'Number of melkbroodjes: 5'
# and melkbroodjes(23) returns 'Number of melkbroodjes: veel'
def melkbroodjes(count):
    return

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lists and loops

# COMMAND ----------

# lists
colors = ['red', 'blue', 'green', "violet"]
print(colors[0])
print(colors[2])
print(colors)

# COMMAND ----------

# slices- python starts counting at 0
print(colors[0])
print(colors[1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### For-loops

# COMMAND ----------

# As the list is being looped though, the variable is updated to the next element and temporarily is assigned the variable name blablabla
for blablabla in colors:
    print(blablabla * 2)
    print(blablabla.upper())
    print("---"*10)

# COMMAND ----------

type(colors)

# COMMAND ----------

# pay attention what you loop through, the type is important!
red_string = 'red'
for letter in red_string:
    print(letter)

# COMMAND ----------

# also naming is important ;)
a = 'red'
for e in a:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### in

# COMMAND ----------

colors = ['red', 'blue', 'green', "violet"]
red_string = 'red'

# COMMAND ----------

# colors = 'red'
colors == 'red'

# COMMAND ----------

# 'red' in colors
# 'r' in red_string

# 'yellow' in colors
# 'y' in red_string

# COMMAND ----------

if 'red' in colors:
    print('Found red')
elif 'blue' in colors:
    print('Found blue')
else:
    print('fngjdf')

# if 'red ' in colors:
#     print('Found red')

# COMMAND ----------

for color in colors:
    if 'red' == color:
        print('Found red')
    elif 'blue' == color:
        print('Found blue')
    else:
        print('fngjdf')

# COMMAND ----------

# MAGIC %md
# MAGIC Here are some other common list methods.
# MAGIC
# MAGIC - list.append(elem) -- adds a single element to the end of the list. Common error: does not return the new list, just modifies the original.
# MAGIC - list.insert(index, elem) -- inserts the element at the given index, shifting elements to the right.
# MAGIC - list.extend(list2) adds the elements in list2 to the end of the list. Using + or += on a list is similar to using extend().
# MAGIC - list.index(elem) -- searches for the given element from the start of the list and returns its index. Throws a ValueError if the element does not appear (use "in" to check without a ValueError).
# MAGIC - list.remove(elem) -- searches for the first instance of the given element and removes it (throws ValueError if not present)
# MAGIC - list.sort() -- sorts the list in place (does not return it). (The sorted() function shown later is preferred.)
# MAGIC - list.reverse() -- reverses the list in place (does not return it)
# MAGIC - list.pop(index) -- removes and returns the element at the given index. Returns the rightmost element if index is omitted (roughly the opposite of append()).

# COMMAND ----------

colors.append('yellow')
print(colors)
# colors.remove('yellow')
# print(colors)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise

# COMMAND ----------

# A. Write a loop that:
# Iterates from 1 to 20.
# Prints "Even" for even numbers and "Odd" for odd numbers.
# Prints the square of each number
# +++your code here+++

# B. Write a function check_number(num) that:
# Prints "Positive" if the number is greater than 0.
# Prints "Negative" if the number is less than 0.
# Prints "Zero" if the number is 0.

def check_num(num):
    # +++your code here+++
    return

# C. Write a function double_numbers(numbers) that doubles each number in the input list.
# double_numbers([1, 2, 3])  # Output: [2, 4, 6]

# COMMAND ----------

# D. Let's fix some code
# The following function contains errors. Debug and fix it so that it returns the sum of squares of all even numbers in the input list.
# sum_of_squares([1, 2, 3, 4]) 
def sum_of_squares(numbers):
    total = 0
    for num in numbers:
        if num % 2:
            total += num ** 2
    return total

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dictionaries
# MAGIC Python's efficient key/value hash table structure is called a "dict". The contents of a dict can be written as a series of key:value pairs within braces { }, e.g. ```dict = {key1:value1, key2:value2, ... } ```. The "empty dict" is just an empty pair of curly braces {}.
# MAGIC
# MAGIC dictionaries can be easily looked up by using square brackets. dict['key_name']

# COMMAND ----------

colors = {
    'red': '#FF0000',
    'green': '#00FF00',
    'blue': '#0000FF'
}
colors

# COMMAND ----------

# Initialize an empty dictionary
colors = {}

# Add key-value pairs to the dictionary
colors['red'] = '#FF0000'
colors['green'] = '#00FF00'
colors['blue'] = '#0000FF'

print(colors)

# Accessing a value using a key
print(colors['red']) 

# Update the value for an existing key
colors['red'] = '#FF6347'  # Update 'red' to a tomato red
print(colors['red'])


# COMMAND ----------

# Check if a key exists in the dictionary
print('yellow' in colors)  # False

# # Use a safe way to check and access a value to avoid KeyError
if 'yellow' in colors:
    print(colors['yellow'])
else:
    print('Key "yellow" does not exist')

# # Using the get() method to safely retrieve a value
print(colors.get('yellow'))

# # Add a new key-value pair for yellow
colors['yellow'] = '#FFFF00'
# print(colors)  # {'red': '#FF6347', 'green': '#00FF00', 'blue': '#0000FF', 'yellow': '#FFFF00'}
colors.get('yellow')

# COMMAND ----------

# by default, iterating over a dict iterates over its keys
for color in colors:
    print(color)

# to get the list of keys 
print(colors.keys())

# to get the list of values 
print(colors.values())

# to get the list of keys and values, a tuple will be returned (key, value)
print(colors.items())

# COMMAND ----------

# MAGIC %md
# MAGIC - **keys** 
# MAGIC   - must be unique in a dictionary 
# MAGIC   - must be strings/numbers or tuples (cannot be lists or dictionaries)
# MAGIC - **values**
# MAGIC   - can take on any python type (strings, numbers, lists, other dictionaries...)
# MAGIC
# MAGIC

# COMMAND ----------

# a single dictionary can mixed key-value types
example_dictionary = {
    'string_type' : 'string',
    123 : 456,
    True : False,
    'True' : False,
    'list_type': ['a', 'b', 'c'],
    'dictionary_type': {'key1': 'value1', 'key2': 'value2'}
}
example_dictionary.get('dictionary_type')

# COMMAND ----------

# getting a value in a nested dictionary
example_dictionary['dictionary_type'].get('key1')

# COMMAND ----------

# Invalid examples
# example_dictionary = {[1, 2]: 'list'} 
# example_dictionary = {{1, 2}: 'list'} 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise

# COMMAND ----------

# A. Create a dictionary 
# Create a dictionary called person with the following keys and values:
# 'name': 'Alice'
# 'age': 30
# 'city': 'Amsterdam'
# Print the entire dictionary.
# Iterate through the dictionary and print the key-value pairs

# B. Accessing Dictionary Items
# Using the person dictionary, print the values for the keys 'name' and 'city'.
# Assign the value of the 'age' key to a variable called age.
# Try to access a key that doesn’t exist (e.g., 'country') and handle the error.

# C. Adding a Nested Dictionary
# Add a new nested dictionary under the key 'address' with the following key-value pairs:
# 'street': 'Herengracht 435'
# 'zip': '1017BR'
# Print the person dictionary, including the nested 'address' dictionary.

# D. Dictionary with Lists as Values
# Add a key 'hobbies' to the person dictionary with a list of hobbies:
# 'reading', 'cycling', 'painting'
# Print all of Alice’s hobbies.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sneak peak Debugging - short example

# COMMAND ----------

def add_numbers(a, b):
    result = a + c
    return result

# print(add_numbers(5, 10))  

# COMMAND ----------

def calculate_average_ages(data):
    """
    Given a dictionary where keys are city names and values are lists of ages,
    calculate and return the average age for each city.
    """
    results = {}
    for city, ages in data.items():
        average_age = sum(ages) / len(ages)
        results[city] = round(average_age, 2)

    return results

# Sample data to debug
data_input = {
    "Amsterdam": [25, 30, 35],
    # "Rotterdam": [],
    "Utrecht": [40, 50, 60],
    # "Eindhoven": [20, "thirty", 40]
}

calculate_average_ages(data_input)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise

# COMMAND ----------

# We have a funciton that processes an order and calculates the total cost.
# Given process_order(["apple", "banana", "grape", "mango", "orange"])
# The expected output is:
# Item 'mango' is not available in the menu.

# Order Summary:
# Items Ordered: ['apple', 'banana', 'grape', 'mango', 'orange']
# Prices: [1.25, 0.75, 2.5, 1.1]
# Total Cost: $5.60

# Can you fix the function?

def process_order(order_list):
    # Dictionary of available items and their prices
    menu = {
        "apple": 1.25,
        "banana": 0.75,
        "orange": 1.10,
        "grape": 2.50,
        "watermelon": 3.00
    }

    # List to store the prices of each item in the order
    order_prices = []
    total_cost = "0"

    # Process each item in the order
    for item in order_list:
        if item in menu:
            price = menu[item]
            order_prices.append(price)
            total_cost += price  # Type compatibility issue here
        else:
            print(f"Item '{item}' is not available in the menu.")

    # Display order summary
    print("Order Summary:")
    print(f"Items Ordered: {order_list}")
    print(f"Prices: {order_prices}")
    print(f"Total Cost: {total_cost}")

# Test the function with sample input
process_order(["apple", "banana", "grape", "mango", "orange"])


# COMMAND ----------

# MAGIC %md
# MAGIC ## 