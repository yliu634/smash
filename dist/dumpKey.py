import os, re, sys

def main(argv):
    f = open("keys.txt", 'r')
    osd = int(argv[0], 10)
    N = int(argv[1], 10)
    # print("osd is: "+ str(osd))
    of = open("dump_osd_" + str(osd) + ".txt", 'w')
    key = f.readline()
    n = 0
    while key and n < N:
        p = os.popen('ceph osd map test ' + str(key))
        x = p.readlines()
        query = re.search("\[.*?\]", x[0], re.I|re.M)
        x = list(map(int, query.group()[1:-1].split(',')))
        # print(x[0] + x[1])
        print(x)
        if osd in x:
            of.write(str(key))
        key = f.readline()
        n = n + 1
    f.close()
    of.close()

if __name__ == "__main__":
    main(sys.argv[1:])
