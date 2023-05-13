while True:
    try:
        line = input()
        line = line.split()
        for w in line:
            w = ''.join(filter(str.isalpha, w)).lower()
            print(w,1,sep='\t',end='\n')
    except:
        break
