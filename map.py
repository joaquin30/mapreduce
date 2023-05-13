while True:
    try:
        line = input()
        line = line.split()
        for w in line:
            print(w,1,sep='\t',end='\n')
    except:
        break
