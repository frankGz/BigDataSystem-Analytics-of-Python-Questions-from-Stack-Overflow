import numpy as np
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import * 
from pyspark.ml.linalg import *
from ast import literal_eval
import json
import sys

sc = SparkContext()

def cos_sim(a,b):
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))


def getIdVector(idv):
    try:
        idv = idv.split('=')
        id = idv[1].split(',')[0]
        vec = idv[-1][12:-1]
    except Exception:
        return None, None
    return (id, literal_eval(vec))

def build_sparse_vec(t):
    size = t[0]
    dic = t[1]
    indices = []
    values = []
    for k in sorted(dic.keys()):
        indices.append(k)
        values.append(dic[k])
    return SparseVector(size, indices, values)

def query(str):
    words = str.split()
    print(words)
    with open('vocabulary.txt', 'r') as f:
        dic = f.read().split('\n')[:-1]
    size = len(dic)
    d = {}
    for word in words: 
        d[word] = d.get(word, float(0)) + float(1)
    
    id_c = []
    for word in d:
        if word in dic:
            index = dic.index(word)
            count = d[word]
            id_c.append((index, count))

    id_c = sorted(id_c, key=lambda x: x[0])
    indices = [i[0] for i in id_c]
    values = [i[1] for i in id_c]
    static_vector = SparseVector(size, indices, values)
    rdd = sc.textFile('/app/Documents/EECS4415_project/data/id_features/part-00000')
    rdd = rdd.map(lambda x: getIdVector(x))
    rdd = rdd.map(lambda x: (x[0], build_sparse_vec(x[1])))
    rdd = rdd.map(lambda x: (x[0], cos_sim(DenseVector(x[1].toArray()), static_vector)))
    return [x[0] for x in rdd.top(10, key=lambda x: x[1])]

if __name__ == "__main__":
    print(query(sys.argv[1]))