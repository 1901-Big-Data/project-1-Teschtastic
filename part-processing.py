#!/usr/local/bin/python3
#import matplotlib.pylab as plt

def main():
    processPartQ1()
    processPartQ2()
    processPartQ3()
    processPartQ4()

def processPartQ1():
    out = open("output-Q1", "w")
    with open("part-r-00000-Q1", "r") as file:
        for line in file:
            typ, val = line.split(":")
            out.write(typ + " " + val)

def processPartQ2():
    d = dict()
    out = open("output-Q2", "w")
    with open("part-r-00000-Q2", "r") as file:
        for line in file:
            typ, val = line.split("\t")
            val = str(val).strip("\n")
            if typ in d:
                d[typ].append(val)
            else:
                d[typ] = [val]
        for typ, val in d.items():
            out.write('{}\t{}\n'.format(typ, val))

def processPartQ3():
    d = dict()
    out = open("output-Q3", "w")
    with open("part-r-00000-Q3", "r") as file:
        for line in file:
            typ, val = line.split("\t")
            val = str(val).strip("\n")
            if typ in d:
                d[typ].append(val)
            else:
                d[typ] = [val]
        for typ, val in d.items():
            out.write('{}\t{}\n'.format(typ, val))

def processPartQ4():
    d = dict()
    out = open("output-Q4", "w")
    with open("part-r-00000-Q4", "r") as file:
        for line in file:
            typ, val = line.split("\t")
            val = str(val).strip("\n")
            if typ in d:
                d[typ].append(val)
            else:
                d[typ] = [val]
        for typ, val in d.items():
            out.write('{}\t{}\n'.format(typ, val))

if __name__ == "__main__":
    main()