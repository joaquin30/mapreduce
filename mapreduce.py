from subprocess import Popen, PIPE
import sys


def die(msg):
    print("ERROR:", msg, """USE:
mapreduce [map OR reduce] [path_to_script] [source_file] [destination_file]""",
    sep="\n", file=sys.stderr)
    sys.exit(1)


def fmap(script, src, dst):
    print("map", script, src, dst)


def freduce(script, src, dst):
    print("reduce", script, src, dst)


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
