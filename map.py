import sys
while True:
    try:
        line = input()
        key, value = line.split('\t')
        for w in value.split():
            w = "".join(filter(str.isalnum, w)).lower()
            print(f"{w}\t1\n", end="")
    except:
        break
