#!/usr/bin/env python

# file: test.py
# summary: test case
# author: caosiyang <csy3228@gmail.com>
# date: 2013/07/16

from gridfs_handler import GridfsHandler 


def test():
    h = GridfsHandler("127.0.0.1", 27017, "pic", "fs")
    filepath = './logo001.png'
    retval, id, md5 = h.put(filepath)
    print "[put] retval: %d, id: %s, md5: %s" % (retval, id, md5)
    if retval == 0:
        filecontent = h.get(filepath)
        if filecontent:
            print "[get] content length: %d" % len(filecontent)
    #h.delete(id)
    h.close()


def stability_test():
    fd = open('log', 'a')
    h = GridfsHandler("127.0.0.1", 27017, "pic", "fs")
    for i in range(100000):
        print >> fd, i
        retval, id, md5 = h.put("./logo001.png")
        print >> fd, "retval: %d, id: %s, md5: %s" % (retval, id, md5)
        if id:
            filecontent = h.get(id)
            if filecontent:
                print >> fd, "content length: %d" % len(filecontent)
        fd.flush()
        time.sleep(1)
    h.close()
    fd.close()


if __name__ == "__main__":
    test()
