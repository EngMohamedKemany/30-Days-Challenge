def matchingStrings(strings, queries):
    # Write your code here
    n = len(strings)
    hashmap = {}

    for i in range(n):
        string = strings[i]
        hashmap[string] = 1 if string not in hashmap else hashmap[string] + 1

    q = len(queries)
    result = []
    for j in range(q):
        string = queries[j]
        result.append(0 if string not in hashmap else hashmap[string])
        
    return result