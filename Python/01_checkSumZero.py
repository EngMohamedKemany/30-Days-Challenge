# Online Python compiler (interpreter) to run Python online.
# Write Python 3 code in this online editor and run it.
# Problem: Find out if any pair in the list adds up to zero
import random
randomlist = []
for i in range(0,6):
    n = random.randint(-10,10)
    randomlist.append(n)
print(randomlist) # Generating random list to play as a new sample case each run

def checkSumZero(lst):
    lst = sorted(lst, key=abs) #Sorting the input on the absolute value ex: [-5, 5, 7, -10, 10] O(n): nlogn
    ptr_one, ptr_two = 0, 1
    while ptr_two < len(lst): # A two pointer approach
        total = lst[ptr_one] + lst[ptr_two]
        if total == 0:
            print(lst[ptr_one], lst[ptr_two])
            return True
        else:
            ptr_one += 1
            ptr_two += 1
    print("NO CLUE")
    return False
checkSumZero(randomlist)
