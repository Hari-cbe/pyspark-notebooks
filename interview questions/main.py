#1 Write a python program to find the common char between strings
# a = 'hari prasath'
# b = 'ellakiya'

# def common_char(a,b):
#     return set(a.replace(" ","")) & set(b.replace(" ",''))  

# print(common_char(a,b))
# 
#  
#2 frequency of words in a string

# string = 'hari prasath'

# def freq_words(string):
#     dicto = {}
#     for i in string:
#         if i not in dicto:
#             dicto[i] = 1
#         else:
#             dicto[i] += 1
#     return dicto
# result = freq_words(string)
#print(sorted(result.items(), key=lambda x:x[1],reverse=True)[0])

# 3 combain 2 lists into dict

# lst1 = [1,2,3,4,5]
# lst2 = [6,7,8,9,10]

# def lst_to_dict(lst1, lst2):
#     dicto = {}
#     for i,j in zip(lst1,lst2):
#         dicto[i] = j
#     return dicto

# print(lst_to_dict(lst1,lst2))

# 
#Question 4: List Flattening and Duplicates
#Write a function that takes a nested list of integers (which can be arbitrarily deep) and returns a flattened list with all duplicate values removed, maintaining the order of first occurrence. For example:

#flatten_and_dedupe([[1, 2], [2, 3, [4, 5]], [5, 6]]) 
# Should return [1, 2, 3, 4, 5, 6]

# def flatten_and_dedupe(ns_lst):
#     lst = []
#     seen = set()

#     def helper(ns_lst):
#         for i in ns_lst:
#             if isinstance(i,list):
#                     helper(i)
#             else:
#                 if i not in lst:
#                     lst.append(i)
#                     # seen.add(i)
    
#     helper(ns_lst)
#     return lst

# print(flatten_and_dedupe([[1, 2,[6,7]], [2, 3, [4, 5]], [5, 6]]))
# def total():
#     total = 0
#     for i in range(1,65):
#         if i == 1:
#             total += 1
#         else:
#             print(i)
#             total += 2 ** (i - 1) 
#     return total

# print(total())

# import time

# def timeit(func):
#     def wrapper(*args, **kwargs):
#         start_time = time.time()
#         result = func(*args, **kwargs)
#         end_time = time.time()
#         print(f"[INFO] {func.__name__} is executed in {start_time - end_time:.2f}s")
#         return result
#     return wrapper


# @timeit
# def extract_data():
#     time.sleep(10)
#     return [1,3,4,5]


# print(extract_data())


# string = 'hari     prasth      is  a data engineer'
# lst  = ' '.join([x for x in string.split(' ') if x.isalnum()])


# print(lst)

# -----Reverse a string with in-built function 
# 
# string = 'raman'
# def reverse_string(s : str) -> str:
#     reversed_str = '' 
#     for i in s:
#         reversed_str = i + reversed_str
#     return reversed_str

# reverse_string(string)

# -----generators

# def fibo(n):
#     lst = []
#     x, y = 0,1 
#     for i in range(n):
#         lst.append(x)
#         x,y = y,x+y
#     return lst 

# fibo(4)

# def fibo(n):
#     x, y = 0,1 
#     for i in range(n):
#         yield x
#         x,y = y,x+y

# [i for i in fibo(4)]


# Infosys vantha sutan sethan repeatu Recur Syndrome
# def recur_syndrome(n):
#     n = n
#     while True:
#         print(n)
#         reversed_n = str(n)[::-1]

#         sumed = n + int(reversed_n)
#         if str(sumed) == str(sumed)[::-1]:
#             return sumed 
#         else:
#             n = sumed

# print(recur_syndrome(97))

# Infosys find the first sum of 2 == 10 

# import time

# lst = [1,4,7,8,1,14,6,18]

# start = time.time()

# n = len(lst)
# i = 0

# while i < n-1:
#     # print(i, i+1)
#     if lst[i] + lst[i+1] == 10:
#         print(lst[i], lst[i+1])
#         break 
#     i += 1
# else:
#     print("there is no occurance")

# print(f"time taken {time.time() - start}")


# def checktime(func):
    
#     def wrapper(*args, **kwargs):
#         start_time = time.time()
#         result = func(*args, **kwargs)
#         end_time = time.time()
#         print(result)
#         print(f" time taken {end_time - start_time}")
#         return result
#     return wrapper

# @checktime
# def find_sum(lst):
#     for i in range(len(lst)):
#         for j in range(i+1, len(lst)):
#             if lst[i] + lst[j] == 10:
#                 return (lst[i],lst[j])
#     else:
#         return "There is no occurance"

    
# find_sum(lst)

# Write a program that handles division by zero using try-except.

''' 
    What is the difference between if and try exception is -> both are control flow statements
    but try catch - handles the exception
'''

# n = 10
# try:
#     n / 5 // 3 - 4 ( 5)
# except ZeroDivisionError:
#     print(" Divide BY ZERO !!")
# except Exception as e :
#     print(f"the exception is {e}")
# finally:
#     print("This block will be executed---- finally")
    
# Given a string, count how many vowels it has.

# string = "Given a string, count how many vowels it has"

# print(sum([1 for i in string.lower() if i in ['a','e','i','o','u']]))

# count = 0

# for i in string.lower():
#     if i in ['a','e','i','o','u']:
#         count += 1
# print(count)



# Reverse a list without using built-in functions.

# lst = [1,2,3,4,5]

# print(sorted(lst,reverse=True))

# lst = [1,2,3,4,5]
# rev_lst = []
# n = len(lst)
# print(n)
# for index,i in enumerate(range(n-1,-1,-1)):
#     poped_char = lst.pop(i)
#     rev_lst.append(poped_char)

# print(rev_lst)

# lst = ["sql","scala","pyTHon","javA","java","pyspark"]

# rev_lst = []

# for i in range(len(lst)-1,-1,-1):
#     poped = lst.pop(i).lower()
#     if poped not in rev_lst:
#         rev_lst.append(poped)

# print([x[0].upper() + x[1:].lower() for x in rev_lst])
