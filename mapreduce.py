from subprocess import run
import sys


def die(msg):
    print("Error:", msg, """Use:
python mapreduce.py [map OR reduce] [path_to_script] [source_file] [destination_file]""",
    sep="\n", file=sys.stderr)
    sys.exit(1)


def warn(msg):
    print("Warning:", msg, sep="\n", file=sys.stderr)


def getkv(tsv):
    kvs = []
    for line in tsv.split('\n'):
        if len(line) == 0:
            continue
        kv = tuple(line.split('\t'))
        if len(kv) != 2:
            die("TSV Format malformed.")
        kvs.append(kv)
    return kvs


def fmap(script, src, dst):
    data = None
    try:
        with open(src, encoding="utf8") as f:
            data = f.read()
    except:
        die("Source file doesn't exist or can't be read.")

    data = data.split('\n')
    inp = ""
    for ind, line in enumerate(data):
        inp += f"{ind}\t{line}\n"
    
    out = None
    try:
        out = run(script.split(), input=inp.encode(),
            capture_output=True).stdout.decode("utf8")
    except:
        die("The script doesn't exist or failed in execution.")

    out = getkv(out)
    out.sort()
    try:
        with open(dst, "w", encoding="utf8") as f:
            for k, v in out:
                f.write(f'{k}\t{v}\n')
    except:
        die("Unable to write to destination file.")


def freduce(script, src, dst):
    data = None
    try:
        with open(src, encoding="utf8") as f:
            data = f.read()
    except:
        die("Source file doesn't exist or can't be read.")

    data = getkv(data)
    if len(data) == 0:
        warn("Source file empty.")
        sys.exit()

    key = data[0][0]
    inp = ""
    out = ""
    for k, v in data:
        if k == key:
            inp += f"{k}\t{v}\n"
        else:
            try:
                out += run(script.split(), input=inp.encode(),
                            capture_output=True).stdout.decode("utf8")
            except:
                die("The script doesn't exist or failed in execution.")

            inp = f"{k}\t{v}\n"
            key = k

    try:
        out += run(script.split(), input=inp.encode(),
                    capture_output=True).stdout.decode("utf8")
    except:
        die("The script doesn't exist or failed in execution.")

    out = getkv(out)
    out.sort()

    try:
        with open(dst, "w", encoding="utf8") as f:
            for k, v in out:
                f.write(f'{k}\t{v}\n')
    except:
        die("Unable to write to destination file")


def main():
    if len(sys.argv) != 5:
        die("Incorrect number of arguments")

    if sys.argv[1] == "map":
        fmap(*sys.argv[2:])
    elif sys.argv[1] == "reduce":
        freduce(*sys.argv[2:])
    else:
        die("Option not recognized")


if __name__ == "__main__":
    main()
