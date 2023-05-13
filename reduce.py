key = ""
value = 0
first = True

while True:
    try:
        line = input()
        k, v = line.split()
        if first:
            key = k
            first = False
        else:
            assert key == k
        value += int(v)
    except:
        break

print(f'{key}\t{value}')
