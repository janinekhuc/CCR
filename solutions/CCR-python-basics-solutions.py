# Databricks notebook source
# MAGIC %md
# MAGIC # Python basics
# MAGIC
# MAGIC Goal: Cover basics for you to be able to better start understanding and debbugging code

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variables and data types

# COMMAND ----------

# strings-----------------------------------
string_variable = '12345'
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

# slices- python starts counting at 0
print(string_variable[0])
print(string_variable[1])

# '12345' in string_variable
# '12' in string_variable

# COMMAND ----------

# string formatting f'strings-----------------------------------
# of what are the following variables?
value = 2.791514
print(f'exact value = {value}')
# print(f'approximate value = {value:.2f}')  # approximate value = 2.79
print(f'string_variable = {string_variable}')
print(f'integer_variable = {integer_variable}')
print(f'float_variable = {float_variable}')
print(f'is_fun = {is_fun}')

# COMMAND ----------

# MAGIC %md
# MAGIC Here are some of the most common string methods. A method is like a function, but it runs "on" an object. If the variable s is a string, then the code s.lower() runs the lower() method on that string object and returns the result (this idea of a method running on an object is one of the basic ideas that make up Object Oriented Programming, OOP). Here are some of the most common string methods:
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
# SOLUTIONS
def melkbroodjes(count):
    """
    Returns a string describing the number of melkbroodjes.
    If the count is 10 or more, returns 'veel' instead of the count.
    """
    if count >= 10:
        return 'Number of melkbroodjes: veel'
    else:
        return f'Number of melkbroodjes: {count}'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lists and loops

# COMMAND ----------

# lists
colors = ['red', 'blue', 'green']
print(colors[0])
print(colors[2])
print(len(colors))

# COMMAND ----------

# slices- python starts counting at 0
print(colors[0])
print(colors[1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### For-loops

# COMMAND ----------

# for loops
for color in colors:
    print(color)

# COMMAND ----------

# pay attention what you loop through
red_string = 'red'
for letter in red_string:
    print(letter)

# COMMAND ----------

# MAGIC %md
# MAGIC ### in

# COMMAND ----------

# 'red' in colors
# 'r' in red_string

# 'yellow' in colors
# 'y' in red_string

# COMMAND ----------

if 'red' in colors:
    print('Found red')

# if 'red ' in colors:
#     print('Found red')

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

# SOLUTION
# Loop from 1 to 20
for num in range(1, 21):
    if num % 2 == 0:
        print(f"{num}: Even")
    else:
        print(f"{num}: Odd")
    
    # Print the square of the number
    print(f"Square: {num ** 2}")

# B. Write a function check_number(num) that:
# Prints "Positive" if the number is greater than 0.
# Prints "Negative" if the number is less than 0.
# Prints "Zero" if the number is 0.

def check_num(num):
    # +++your code here+++
    return

# SOLUTION
def check_num(num):
    """Prints whether the input number is Positive, Negative, or Zero."""
    if num > 0:
        print("Positive")
    elif num < 0:
        print("Negative")
    else:
        print("Zero")

# C. Write a function double_numbers(numbers) that doubles each number in the input list.
# double_numbers([1, 2, 3])  # Output: [2, 4, 6]
# SOLUTION
def double_numbers(numbers):
    """Given a list of numbers, return a new list with each number doubled."""
    return [number * 2 for number in numbers]

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

# Solution:
def sum_of_squares(numbers):
    total = 0
    for num in numbers:
        if num % 2 == 0:  # Check for even numbers
            total += num ** 2
    return total

# Example usage
# print(sum_of_squares([1, 2, 3, 4])) 
sum_of_squares([1, 2, 3, 4]) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dictionaries

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
print(colors['red'])  # #FF6347

# COMMAND ----------

# Check if a key exists in the dictionary
print('yellow' in colors)  # False

# Use a safe way to check and access a value to avoid KeyError
if 'yellow' in colors:
    print(colors['yellow'])  # Won't execute
else:
    print('Key "yellow" does not exist')

# Using the get() method to safely retrieve a value
print(colors.get('yellow'))  # None

# Add a new key-value pair for yellow
colors['yellow'] = '#FFFF00'
print(colors)  # {'red': '#FF6347', 'green': '#00FF00', 'blue': '#0000FF', 'yellow': '#FFFF00'}

# COMMAND ----------

# by default, iterating over a dict iterates over its keys
for key in colors:
    print(key)

# to get the list of keys 
print(colors.keys())

# to get the list of keys 
print(colors.values())

# to get the list of keys and values, a tuple will be returned (key, value)
print(dict.items())

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

# SOLUTION
# Create the dictionary with specified keys and values
person = {
    'name': 'Alice',
    'age': 30,
    'city': 'Amsterdam'
}

# Print the entire dictionary
print(person)

# Iterate through the dictionary and print the key-value pairs
for key, value in person.items():
    print(f"{key}: {value}")



# B. Accessing Dictionary Items
# Using the person dictionary, print the values for the keys 'name' and 'city'.
# Assign the value of the 'age' key to a variable called age.
# Try to access a key that doesn’t exist (e.g., 'country') and handle the error.

# SOLUTION
# Access the values for the keys 'name' and 'city'
print(person['name'])  # Prints: Alice
print(person['city'])  # Prints: Amsterdam

# Assign the value of the 'age' key to a variable called 'age'
age = person['age']
print(age)  # Prints: 30

# Try to access a key that doesn’t exist and handle the error
try:
    print(person['country'])  # This will raise a KeyError
except KeyError:
    print("Key 'country' not found!")  # Handle the error gracefully


# C. Adding a Nested Dictionary
# Add a new nested dictionary under the key 'address' with the following key-value pairs:
# 'street': 'Herengracht 435'
# 'zip': '1017BR'
# Print the person dictionary, including the nested 'address' dictionary.
# SOLUTION
# Add a nested dictionary under the 'address' key

person['address'] = {
    'street': 'Herengracht 435',
    'zip': '1017BR'
}

# Print the updated person dictionary
print(person)

# D. Dictionary with Lists as Values
# Add a key 'hobbies' to the person dictionary with a list of hobbies:
# 'reading', 'cycling', 'painting'
# Print all of Alice’s hobbies.
# SOLUTION
# Add a key 'hobbies' with a list of hobbies
person['hobbies'] = ['reading', 'cycling', 'painting']

# Print all of Alice’s hobbies
print(person['hobbies'])

# COMMAND ----------

def calculate_average_ages(data):
    """
    Given a dictionary where keys are city names and values are lists of ages,
    calculate and return the average age for each city.
    """
    results = {}
    
    for city, ages in data.items():
        if not ages:
            continue
        
        try:
            average_age = sum(ages) / len(ages)
            results[city] = round(average_age, 2)
        except (ZeroDivisionError, TypeError):
            results[city] = None

    return results

# COMMAND ----------

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
    total_cost = 0.0  # Initialize as float

    # Process each item in the order
    for item in order_list:
        if item in menu:
            price = menu[item]
            order_prices.append(price)
            total_cost += price  # Correctly add float values
        else:
            print(f"Item '{item}' is not available in the menu.")

    # Display order summary
    print("Order Summary:")
    print(f"Items Ordered: {order_list}")
    print(f"Prices: {order_prices}")
    print(f"Total Cost: {total_cost:.2f}")  # Format to 2 decimal places

# Test the function with sample input
process_order(["apple", "banana", "grape", "mango", "orange"])

