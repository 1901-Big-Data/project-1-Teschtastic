#!/usr/local/bin/python3
from matplotlib import pyplot as plt
import array
import random
from random import randrange

def main():
    processPartQ1()
    processPartQ2()
    processPartQ3()
    processPartQ4()

def processPartQ1():
    list1 = list()
    list2 = list()
    list3 = list()
    list4 = list()
    out = open("output-Q1", "w")
    with open("part-r-00000-Q1", "r") as file:
        for line in file:
            typ, val = line.split(":")
            val = str(val).strip("\t \n")
            out.write(typ + "\t" + val + "\n")
            list1.append(typ)
            list2.append(float(val))
        for x in range(20):
            rand_ind = randrange(len(list1))
            list3.append(list1[rand_ind])
            list4.append(list2[rand_ind])

        plt.bar(list3, list4)
        plt.xticks(range(20), list3, rotation='45')
        plt.margins(0.05)
        plt.subplots_adjust(bottom=0.15)
        plt.show()

def processPartQ2():
    d = dict()
    out = open("output-Q2", "w")
    with open("part-r-00000-Q2", "r") as file:
        for line in file:
            typ, val = line.split("\t")
            val = str(val).strip("\n")
            if typ in d:
                d[typ].append(float(val))
            else:
                d[typ] = [float(val)]
                
        for typ, val in d.items():
            out.write('{}\t{}\n'.format(typ, val))

        lists1 = d.get("United States primary")
        lists2 = d.get("United States secondary")
        lists3 = d.get("United States tertiary")

        plt.plot(range(2000, 2015), lists1, "r--")
        plt.title("United States primary")
        plt.xlabel("Year")
        plt.ylabel("Percent change")
        plt.show()

        plt.plot(range(2000, 2014), lists2, "r--")
        plt.title("United States secondary")
        plt.xlabel("Year")
        plt.ylabel("Percent change")
        plt.show()

        plt.plot(range(2000, 2015), lists3, "r--")
        plt.title("United States tertiary")
        plt.xlabel("Year")
        plt.ylabel("Percent change")
        plt.show()

def processPartQ3():
    d = dict()
    list1 = list()
    list2 = list()
    list3 = list()
    list4 = list()
    out = open("output-Q3", "w")
    with open("part-r-00000-Q3", "r") as file:
        for line in file:
            typ, val = line.split("\t")
            val = str(val).strip("\n")
            if typ in d:
                d[typ].append(float(val))
            else:
                d[typ] = [float(val)]

    for typ, val in d.items():
        out.write('{}\t{}\n'.format(typ, val))
        first = val[0]
        last = val[-1]
        list1.append(typ)
        if first != 0:
            avg = ((last-first)/first)*100
            list2.append(avg)
        else:
            avg = 0
            list2.append(avg)
    for x in range(20):
        rand_ind = randrange(len(list1))
        list3.append(list1[rand_ind])
        list4.append(list2[rand_ind])

    plt.bar(list3, list4)
    plt.xticks(range(20), list3, rotation='65')
    plt.margins(0.05)
    plt.subplots_adjust(bottom=0.15)
    plt.show()

def processPartQ4():
    d = dict()
    list1 = list()
    list2 = list()
    list3 = list()
    list4 = list()
    out = open("output-Q4", "w")
    with open("part-r-00000-Q4", "r") as file:
        for line in file:
            typ, val = line.split("\t")
            val = str(val).strip("\n")
            if typ in d:
                d[typ].append(float(val))
            else:
                d[typ] = [float(val)]

    for typ, val in d.items():
        out.write('{}\t{}\n'.format(typ, val))
        first = val[0]
        last = val[-1]
        list1.append(typ)
        if first != 0:
            avg = ((last-first)/first)*100
            list2.append(avg)
        else:
            avg = 0
            list2.append(avg)
    for x in range(20):
        rand_ind = randrange(len(list1))
        list3.append(list1[rand_ind])
        list4.append(list2[rand_ind])
        
    plt.bar(list3, list4)
    plt.xticks(range(20), list3, rotation='65')
    plt.margins(0.05)
    plt.subplots_adjust(bottom=0.15)
    plt.show()

if __name__ == "__main__":
    main()