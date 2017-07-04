###
# Code by Jeff Rodny 7/4/17
# for Insight Data Engineering Code Challenge
###

import json
import sys
import math
from collections import OrderedDict


# Read in the Batch JSON file
# from: https://stackoverflow.com/questions/12451431/loading-and-parsing-a-json-file-with-multiple-json-objects-in-python
data_batch_P = [] ## list of dictionaries
data_batch_L1 = {} ## dictionary
data_batch_F = [] ## list of dictionaries
data_batch = [] ## list of dictionaries
with open(sys.argv[1]) as json_in_file:
    for line in json_in_file:
        data_batch.append(json.loads(line))
        
##print (data_batch)

for entry in data_batch:
    if 'event_type' in entry:
        if entry['event_type'] == 'purchase': ## equivalent lines: "entry['event_type']" and "entry.get('event_type')" 
            data_batch_P.append(entry)
        elif entry['event_type'] == 'befriend':
            data_batch_F.append(entry)
        elif entry['event_type'] == 'unfriend':
            data_batch_F.append(entry)
    elif 'T' in entry:
        data_batch_L1.update(entry)

        
# save D and T values
CONST_T = int(data_batch_L1['T']) ## equivalent lines: .get('T') and ['T']
CONST_D = int(data_batch_L1['D'])


# Create matrix where index is userID, and at each index is a linked list of 1st degree friends
mat_user_to_friend = {}
# initialize matrix to be used and explained below (Create a matrix where index is userID, and each index is a linked list of purchases)
mat_user_to_purchase = {}
for entry in data_batch_F:
    id1 = int(entry['id1'])
    id2 = int(entry['id2'])
    if entry['event_type'] == 'befriend':
        # make sure the users exist
        mat_user_to_friend.setdefault(id1,[])
        mat_user_to_friend.setdefault(id2,[])
        mat_user_to_purchase.setdefault(id1,[])
        mat_user_to_purchase.setdefault(id2,[])
        # add them to the dictionary
        mat_user_to_friend[id1].append(id2)
        mat_user_to_friend[id2].append(id1)
    if entry['event_type'] == 'unfriend':
        # remove them from the dictionary
        try:
            mat_user_to_friend[id1].remove(id2)
            mat_user_to_friend[id2].remove(id1)
        except:
            print ('Dangnabbit! Yer Batch file tried to remove a user not in the database! (process_log.py line 63)')


# get the friends of a user - unnecessary but looks nice
def get_friends(userID):
    return mat_user_to_friend[userID]


# Create a matrix where index is userID, and each index is a linked list of purchases
for entry in data_batch_P:
    id1 = int(entry['id'])
    mat_user_to_purchase.setdefault(id1,[])
    mat_user_to_friend.setdefault(id1,[])
    mat_user_to_purchase[id1].append(float(entry['amount']))


# get the purchases of a user - unnecessary but looks nice
def get_purchases(userID):
    return mat_user_to_purchase[userID][-CONST_T:]


purchase_list = {}
def recalculate_purchase_lists():
    global purchase_list
    global mat_user_to_friend
    global CONST_T
    global CONST_D
    
    # for each user
    for user in mat_user_to_friend.iterkeys():
        purchase_list[user] = []
        friend_list = get_friends(user)
        visited_users = []
        curDegree = 1
        # create a friends group
        while len(friend_list) > 0:
            # loop through for each friend of the user
            for friend in friend_list:
                # if its a friend we've already visited, skip to next friend, (and remove from the list as below)
                if visited_users.count(friend) > 0:
                    friend_list.remove(friend)
                    continue
                # loop through all of the purchases of that friend
                for each_purchase in get_purchases(friend):
                    purchase_list[user].append(each_purchase)
                    purchase_list[user] = purchase_list[user][-CONST_T:]
                # if we're not at the max degrees then go one level deeper, by adding the friends of this friend to the friend_list
                if curDegree < CONST_D:
                    friend_list.extend(get_friends(friend))
                # remove this friend from the friend_list - we started with a friend list of the user, and the next loop step will
                #   have all of the friends of friends 1 degree away (next loop with be degree 3 etc
                friend_list.remove(friend)
                visited_users.append(friend)
            curDegree += 1

# update the list of purchases per each friend group (AKA do that code right above)
recalculate_purchase_lists()

#################################################################################################################################
# Above code created a purchase_list dictionary, where the key is the user
#  and the value is a list of purchases calculated for the friend group of degree D
#
# Below will read in the Stream file and look for anomolies
######

def calc_outstanding_purchases_with_amount(user, amount):
    ## now that we have purchase lists, we can calc the means and SD
    if len(purchase_list) > 0:
        data_mean = calc_mean(purchase_list[user])
        data_SD = calc_SD(purchase_list[user], data_mean)
        if data_SD == -1:
            return -1
        if amount > data_mean + data_SD * 3:
            return [amount, data_mean, data_SD]
    return -1

def calc_outstanding_purchases(user):
    ## now that we have purchase lists, we can calc the means and SD
    if len(purchase_list) > 0:
        data_mean = calc_mean(purchase_list[user])
        data_SD = calc_SD(purchase_list[user], data_mean)
        if data_SD == -1:
            return -1
        for entry in purchase_list:
            if entry > data_mean + data_SD * 3:
                return [entry, data_mean, data_SD]
    return -1

def calc_mean(data):
    my_sum = 0
    my_cntr = 0
    for entry in data:
        my_sum += entry
        my_cntr += 1
    return my_sum/my_cntr

def calc_SD(data, mean):
    my_sum = 0
    my_cntr = 0
    for entry in data:
        my_sum += (entry - mean) * (entry - mean)
        my_cntr += 1
    if cntr < 3:
        return -1
    return math.sqrt(my_sum / my_cntr)


# Read in the Stream JSON file
data_stream_P = []
data_stream_L1 = {}
data_stream_F = []
data_stream = []
with open(sys.argv[2]) as json_in_file:
    for entry in json_in_file:
        data_stream.append(json.loads(entry))

# create an empty output file
# from: https://stackoverflow.com/questions/12654772/create-empty-file-using-python
open(sys.argv[3], 'a').close()

# start reading the steam file
for entry in data_stream:
    if 'event_type' in entry:
        if entry.get('event_type') == 'purchase': ## equivalent lines: "entry['event_type']" and "entry.get('event_type')" 
            id1 = int(entry['id'])
            mat_user_to_purchase.setdefault(id1,[])
            mat_user_to_friend.setdefault(id1,[])

            [a,b,c] = calc_outstanding_purchases_with_amount(id1, float(entry['amount']))
            if a != -1: # -1 if not anomoly, otherwise it returns the amount
                with open(sys.argv[3], 'a') as outfile:
                    ## Force the order with an ordered dictionary - just passing the dictionary gives wrong order
                    tmpOut_dict = OrderedDict([('event_type',entry.get('event_type')), ('timestamp',entry.get('timestamp')), ('id',entry.get('id')), ('amount',entry.get('amount')), ('mean',str("{0:.2f}".format(b))), ('sd',str("{0:.2f}".format(c)))])
                    json.dump(tmpOut_dict, outfile)
            
            mat_user_to_purchase[id1].append(float(entry['amount']))
            data_batch_P.append(entry)
            
            # recalculate the purchase lists for each friend group - two purchases could come in and we have to recalculate for each
            # this is below as we don't want the new purchase to affect the mean, sd
            recalculate_purchase_lists()
                    
            
        elif entry['event_type'] == 'befriend':
            # first make sure the users exist
            mat_user_to_friend.setdefault(id1,[])
            mat_user_to_friend.setdefault(id2,[])
            mat_user_to_purchase.setdefault(id1,[])
            mat_user_to_purchase.setdefault(id2,[])
            # make friend group modifications
            mat_user_to_friend[id1].append(id2)
            mat_user_to_friend[id2].append(id1)
            data_batch_F.append(entry)
        
            # recalculate the purchase lists for each friend group - two purchases could come in and we have to recalculate for each
            recalculate_purchase_lists()
            
            [a,b,c] = calc_outstanding_purchases_with_amount(id1)
            if a != -1: # -1 if not anomoly, otherwise it returns the entry
                with open(sys.argv[3], 'a') as outfile:
                    ## Force the order with an ordered dictionary - just passing the dictionary gives wrong order
                    tmpOut_dict = OrderedDict([('event_type',entry.get('event_type')), ('timestamp',entry.get('timestamp')), ('id',entry.get('id')), ('amount',entry.get('amount')), ('mean',str("{0:.2f}".format(b))), ('sd',str("{0:.2f}".format(c)))])
                    json.dump(entry, outfile)
                    
            [a,b,c] = calc_outstanding_purchases_with_amount(id2)
            if a != -1: # -1 if not anomoly, otherwise it returns the entry
                with open(sys.argv[3], 'a') as outfile:
                    ## Force the order with an ordered dictionary - just passing the dictionary gives wrong order
                    tmpOut_dict = OrderedDict([('event_type',entry.get('event_type')), ('timestamp',entry.get('timestamp')), ('id',entry.get('id')), ('amount',entry.get('amount')), ('mean',str("{0:.2f}".format(b))), ('sd',str("{0:.2f}".format(c)))])
                    json.dump(entry, outfile)

        elif entry['event_type'] == 'unfriend':
            # make friend group modifications
            try:
                mat_user_to_friend[id1].remove(id2)
                mat_user_to_friend[id2].remove(id1)
                data_batch_F.append(entry)
            except:
                print ('Dangnabbit! Yer stream file tried to remove a user not in the database! (process_log.py line 234)')
                
            # recalculate the purchase lists for each friend group - two purchases could come in and we have to recalculate for each
            recalculate_purchase_lists()
            [a,b,c] = calc_outstanding_purchases_with_amount(id1)
            if a != -1: # -1 if not anomoly, otherwise it returns the entry
                with open(sys.argv[3], 'a') as outfile:
                    ## Force the order with an ordered dictionary - just passing the dictionary gives wrong order
                    tmpOut_dict = OrderedDict([('event_type',entry.get('event_type')), ('timestamp',entry.get('timestamp')), ('id',entry.get('id')), ('amount',entry.get('amount')), ('mean',str("{0:.2f}".format(b))), ('sd',str("{0:.2f}".format(c)))])
                    json.dump(entry, outfile)
            [a,b,c] = calc_outstanding_purchases_with_amount(id2)
            if a != -1: # -1 if not anomoly, otherwise it returns the entry
                with open(sys.argv[3], 'a') as outfile:
                    ## Force the order with an ordered dictionary - just passing the dictionary gives wrong order
                    tmpOut_dict = OrderedDict([('event_type',entry.get('event_type')), ('timestamp',entry.get('timestamp')), ('id',entry.get('id')), ('amount',entry.get('amount')), ('mean',str("{0:.2f}".format(b))), ('sd',str("{0:.2f}".format(c)))])
                    json.dump(entry, outfile)
            
    elif 'T' in entry:
        data_batch_L1.update(entry)

