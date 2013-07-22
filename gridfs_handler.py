#!/usr/bin/env python

# file: gridfs_handler.py
# summary: custom API for MongoDB GridFS
# author: caosiyang <csy3228@gmail.com>
# date: 2013/05/30

import os, sys, random, time
from pymongo.mongo_client import MongoClient
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from pymongo.read_preferences import ReadPreference
from pymongo.database import Database
from pymongo.collection import Collection
from bson.objectid import ObjectId
from gridfs import GridFS


class GridfsHandler:
    """MongoDB GridFS handler.
    """
    def __init__(self, host, port, dbname, bucketname="fs", rsname=None):
        """Connect to MongoDB server.
        If you use replica set, please specify the replica set name.
        """
        self.client = None
        self.host = host
        self.dbname = dbname
        self.bucketname = bucketname
        self.rsname = rsname
        try:
            if rsname:
                self.client = MongoReplicaSetClient(host, replicaSet=rsname)
                self.client.read_preference = ReadPreference.PRIMARY_PREFERRED
                #self.client.read_preference = ReadPreference.SECONDARY_PREFERRED
                #print self.client.seeds
                #print self.client.hosts
                #print self.client.read_preference
                #print self.client.primary
                #print self.client.secondaries
                #print self.client.arbiters
            else:
                self.client = MongoClient(host)
            self.db = Database(self.client, self.dbname)
            self.gridfs = GridFS(self.db, self.bucketname)
        except Exception, e:
            print e
            raise e

    def __del__(self):
        """Close connection.
        """
        self.close()

    def close(self):
        """Close connection.
        """
        if self.client:
            self.client.close()
            self.client = None

    def put(self, filepath):
        """Add a file with specfic filepath.
        Return: code, _id, md5
        code:
            0 - success
           -1 - failed
        """
        try:
            if filepath and os.path.exists(filepath) and os.path.isfile(filepath):
                fd = open(filepath, 'r')
                content = fd.read()
                fd.close()
                id = self._put(filepath, content)
                if id:
                    collname = '%s.files' % self.bucketname
                    coll = Collection(self.db, collname)
                    if coll:
                        doc = coll.find_one({'_id': id}, {'md5': 1})
                        if doc:
                            md5 = doc['md5']
                            return 0, str(id), str(md5)
                        else:
                            print "[ERROR] not found document with id '%s'" % str(id)
                    else:
                        print "[ERROR] not found collection with name '%s'" % collname
                else:
                    print "[ERROR] put file '%s' failed"  % filepath
            else:
                print "[ERROR] not found file '%s'" % filepath
        except Exception, e:
            print e
        return -1, None, None

    def _put(self, filepath, filecontent):
        """Add a file.
        Return: _id
        """
        try:
            id = self.gridfs.put(filecontent, filename=filepath)
            return id
        except Exception, e:
            print e
            return None

    def get(self, filepath):
        """Get the lastest file with specific filepath.
        Return: file content
        """
        try:
            collname = '%s.files' % self.bucketname
            coll = Collection(self.db, collname)
            if coll:
                doc = coll.find_one({'filename': str(filepath)}, sort=[('uploadDate', -1)])
                if doc:
                    id = doc['_id']
                    gout = self.gridfs.get(ObjectId(id))
                    if gout:
                        content = gout.read()
                        gout.close()
                        return content
        except Exception, e:
            print e
            return None

    def delete(self, id):
        """Delete a file with specfic _id.
        Return: True/False
        """
        try:
            self.gridfs.delete(ObjectId(id))
        except Exception, e:
            print e
            raise e

    def drop_database(self):
        """Drop database.
        """
        try:
            if self.client:
                self.client.drop_database(self.dbname)
        except Exception, e:
            print e
            raise e
