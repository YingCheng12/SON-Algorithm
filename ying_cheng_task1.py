from pyspark import SparkContext
import itertools
import os
import time
import sys

# sys.argv[1]
sc = SparkContext('local[*]', 'Task1')
# file_path = '/Users/irischeng/INF553/Assignment/hw2/small2.csv'
file_path = sys.argv[3]
# reviewFile_path = sys.argv[1]
file = sc.textFile(file_path)

support = int(sys.argv[2])
case= int(sys.argv[1])


def find_candidates(iter):
    maxLen = 0
    part = [i for i in iter]
    filter_set = set()
    for i in part:
        maxLen = max(maxLen, len(i))
        for num in i:
            filter_set.add(num)
    # print("maxlen is", maxLen)
    # print(filter_set)
    all_frequent_sets = set()
    all_frequent_sets.add(())
    s = support//2
    # print(s)

    for i in range(1, maxLen+1):

        # print(i)
        # print(filter_set)
        ci_count = {}
        for transaction in part: #（1，2，3)
            # ci_count = {}
            # print(transaction)
            transaction_removed = set()
            for each_transaction in transaction:  #(1)
                # print(each_transaction)
                if each_transaction in filter_set:
                    transaction_removed.add(each_transaction)
            # print(transaction_removed)
            temp_ci_transaction = itertools.combinations(sorted(transaction_removed), i)
            for temp_ci in temp_ci_transaction:
                ##check the subset of temp_ci
                sub_temp_ci_transaction = itertools.combinations(sorted(temp_ci), i-1)
                flag=True
                for sub_temp_ci in sub_temp_ci_transaction:
                    if sub_temp_ci not in all_frequent_sets:
                        flag = False
                        break
                if flag:
                    if temp_ci in ci_count:
                        ci_count[temp_ci] = ci_count[temp_ci]+1
                    else:
                        ci_count[temp_ci] = 1
        # print(ci_count)
        filter_set = set()
        for everyone in ci_count.keys():
            if ci_count[everyone] >= s:
                all_frequent_sets.add(everyone)
                for num in everyone:
                    filter_set.add(num)

        # print(frequent_sets)
    yield all_frequent_sets


def create_key(basket, candidates_list):
    list_key = []
    for candidates in candidates_list:
        # print(candidates)
        flag = True
        for single_item in candidates:
            # print(every_check)
            if single_item not in basket:
                flag = False
                break
        if flag:
            # print(candidates)
            list_key.append(candidates)
    return list_key


def split_comma(s):
    return s.split(",")


def swap(s):
    return (s[1], s[0])

def union(u1, u2):
    return itertools.chain(u1, u2)


if case == 1:
    start = time.perf_counter()
    header = file.first()
    file_RDD = file.filter(lambda row: row != header).map(split_comma).map(lambda s: (s[0], s[1]))
    basket_u_b = file_RDD.aggregateByKey(list(), lambda u, v: itertools.chain(u,[v]), union).map(lambda row: (row[0], list(set(row[1])))).map(lambda s: s[1]).repartition(2)  #rdd
    basket_list_u_b = file_RDD.aggregateByKey(list(), lambda u, v: itertools.chain(u,[v]), union).map(lambda row: (row[0], list(set(row[1])))).map(lambda s: s[1]).collect()  #list

    candidates_u_b = basket_u_b.mapPartitions(lambda x: find_candidates(x))  ##rdd
    candidates_u_b_list = set(candidates_u_b.flatMap(lambda x: x).collect()) ##list to set to remove duplicate

    real_frequent_u_b = basket_u_b.flatMap(lambda basket: create_key(basket, candidates_u_b_list)).map(lambda s: (s, 1)).\
        reduceByKey(lambda a, b: a+b).filter(lambda s: s[1]>= support).filter(lambda s: s[0] is not ()).map(lambda s: s[0]).collect()
    # print(len(real_frequent_u_b))

    dic_output_u_b_can = {}
    for i in candidates_u_b_list:
        if len(i) in dic_output_u_b_can:
            dic_output_u_b_can[len(i)].append(i)
        else:
            dic_output_u_b_can[len(i)] = []
            dic_output_u_b_can[len(i)].append(i)
    # print(dic_output_u_b_can)

    dic_output_u_b_fre = {}
    for i in real_frequent_u_b:
        # print(i)
        # print(type(i))
        if len(i) in dic_output_u_b_fre:
            dic_output_u_b_fre[len(i)].append(i)
        else:
            dic_output_u_b_fre[len(i)] = []
            dic_output_u_b_fre[len(i)].append(i)


    fileObject = open(sys.argv[4], 'w')
    fileObject.write("Candidates:\n")
    for i in range(1, max(dic_output_u_b_can.keys())+1):
        temp_list_can = sorted(dic_output_u_b_can[i])
        for j in temp_list_can:

            fileObject.write(str(j).replace(",)", ")"))
            if j!= temp_list_can[-1]:
                # print(j.index)
                fileObject.write(",")
        fileObject.write("\n")
        fileObject.write("\n")
    fileObject.write("Frequent Itemsets:\n")
    for i in range(1, max(dic_output_u_b_fre.keys())+1):
        # print(len(dic_output_u_b_fre[i]))
        temp_list_fre=sorted(dic_output_u_b_fre[i])
        for j in temp_list_fre:
            fileObject.write(str(j).replace(",)", ")"))
            if j!= temp_list_fre[-1]:
                # print(j.index)
                fileObject.write(",")
        fileObject.write("\n")
        fileObject.write("\n")
    fileObject.close()

    end = time.perf_counter()
    print("Duration:", end-start)

if case == 2:
    start = time.perf_counter()
    header = file.first()
    file_RDD = file.filter(lambda row: row != header).map(split_comma).map(lambda s: (s[0], s[1]))
    basket_b_u = file_RDD.map(swap).aggregateByKey(list(), lambda u, v: itertools.chain(u, [v]), union).map(
        lambda row: (row[0], list(set(row[1])))).map(lambda s: s[1]).repartition(2)
    basket_list_b_u = file_RDD.map(swap).aggregateByKey(list(), lambda u, v: itertools.chain(u,[v]), union).map(lambda row: (row[0], list(set(row[1])))).map(lambda s: s[1]).collect()

    candidates_b_u = basket_b_u.mapPartitions(lambda x: find_candidates(x))  ##rdd
    candidates_b_u_list = set(candidates_b_u.flatMap(lambda x: x).collect())
    # print(candidates_b_u_list)

    real_frequent_b_u = basket_b_u.flatMap(lambda basket: create_key(basket, candidates_b_u_list)).map(
        lambda s: (s, 1)). \
        reduceByKey(lambda a, b: a + b).filter(lambda s: s[1] >= support).filter(lambda s: s[0] is not ()).map(
        lambda s: s[0]).collect()
    # print(real_frequent_b_u)

    dic_output_b_u_can = {}
    for i in candidates_b_u_list:
        if len(i) in dic_output_b_u_can:
            dic_output_b_u_can[len(i)].append(i)
        else:
            dic_output_b_u_can[len(i)] = []
            dic_output_b_u_can[len(i)].append(i)
    # print(dic_output_b_u_can)

    dic_output_b_u_fre = {}
    for i in real_frequent_b_u:
        # print(i)
        # print(type(i))
        if len(i) in dic_output_b_u_fre:
            dic_output_b_u_fre[len(i)].append(i)
        else:
            dic_output_b_u_fre[len(i)] = []
            dic_output_b_u_fre[len(i)].append(i)

    fileObject = open(sys.argv[4], 'w')
    fileObject.write("Candidates:\n")
    for i in range(1, max(dic_output_b_u_can.keys()) + 1):
        temp_list_can = sorted(dic_output_b_u_can[i])
        for j in temp_list_can:
            # print(j)
            fileObject.write(str(j).replace(",)", ")"))
            if j != temp_list_can[-1]:
                # print(j.index)
                fileObject.write(",")
        fileObject.write("\n")
        fileObject.write("\n")
    fileObject.write("Frequent Itemsets:\n")
    for i in range(1, max(dic_output_b_u_fre.keys()) + 1):
        temp_list_fre = sorted(dic_output_b_u_fre[i])
        # print(len(dic_output_b_u_fre[i]))
        for j in temp_list_fre:
            fileObject.write(str(j).replace(",)", ")"))
            if j != temp_list_fre[-1]:
                # print(j.index)
                fileObject.write(",")
        fileObject.write("\n")
        fileObject.write("\n")
    fileObject.close()

    end = time.perf_counter()
    print("Duration:", end - start)


















