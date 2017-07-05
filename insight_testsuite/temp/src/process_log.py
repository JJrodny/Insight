###
# Code by Jeff Rodny 7/4/17
# for Insight Data Engineering Code Challenge
#
# ASSUMES a pre-sorted (based on date) Batch and Stream file. Sorting is costly, esp. for large data.
# ASSUMES we check for anomolies ONLY when a user makes a PURCHASE. We can add in the extra code to
#   check on befriend and unfriend events, (which is not a lot), but assuming we want to maximize speed
#   (and the specification document assumes checking for anomolies 'on purchase'), so we don't check
#   on befriend and unfriend events
# ASSUMES when a friend is added we don't update the purchase_history so the past purchases of the new
#   friend are taken into account, just the new purchases that the new friend makes (This is for speed
#   and convenience). It can be made so when a friend is made we retroactively go back and look through
#   the new friends' old purchases but they are ignored for speed
#
###

import json
import sys
import math
import time
from collections import OrderedDict

# Calculate mean
def calc_mean(data):
    my_sum = 0
    my_cntr = 0
    for entry in data:
        my_sum += entry
        my_cntr += 1
    return my_sum/my_cntr

# Calculate SD (and return -1 if less than 3 entries)
def calc_SD(data, mean):
    my_sum = 0
    my_cntr = 0
    for entry in data:
        my_sum += (entry - mean) * (entry - mean)
        my_cntr += 1
    if my_cntr < 3: # stipulation in the specification document only do for 3 or more purchases
        return -1
    return math.sqrt(my_sum / my_cntr)


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
dict_user_to_friend = {}
# initialize matrix to be used and explained below (Create a matrix where index is userID, and each index is a linked list of purchases)
dict_user_to_purchase = {}
for entry in data_batch_F:
    id1 = int(entry['id1'])
    id2 = int(entry['id2'])
    if entry['event_type'] == 'befriend':
        # make sure the users exist
        dict_user_to_friend.setdefault(id1,[])
        dict_user_to_friend.setdefault(id2,[])
        dict_user_to_purchase.setdefault(id1,[])
        dict_user_to_purchase.setdefault(id2,[])
        # add them to the dictionary
        dict_user_to_friend[id1].append(id2)
        dict_user_to_friend[id2].append(id1)
    if entry['event_type'] == 'unfriend':
        # remove them from the dictionary
        try:
            dict_user_to_friend[id1].remove(id2)
            dict_user_to_friend[id2].remove(id1)
        except:
            print ('Dangnabbit! Yer Batch file tried to remove a user not in the database! (process_log.py line ~90)')

print ('dict_user_to_friend', dict_user_to_friend)

# get the friends of a user - unnecessary but looks nice
def get_friends(userID):
    return list(dict_user_to_friend[userID])


### Create a matrix where index is userID, and each index is a linked list of purchases
##for entry in data_batch_P:
##    id1 = int(entry['id'])
##    dict_user_to_purchase.setdefault(id1,[])
##    dict_user_to_friend.setdefault(id1,[])
##    dict_user_to_purchase[id1] = {}
##    print (time.strptime(entry['timestamp'],"%Y-%m-%d %H:%M:%S"))
##    dict_user_to_purchase[id1][time.strptime(entry['timestamp'],"%Y-%m-%d %H:%M:%S")].append(float(entry['amount']))
    


### get the purchases of a user - unnecessary but looks nice
##def get_purchases(userID):
##    return dict_user_to_purchase[userID][:][-CONST_T:]

######
###
### Summary of Code below: create a list of friend groups,
###   then go through the purchases, one by one, and add that purchase
###   to each friend group in order.
###   That way we dont have to get all purchases of all friends in the friend group
###   and then cut it off that way
###
### We'll create a function that takes in a list of these dictionary entries
###   so that we can call it on batch,
###   and another function so we can call it on each entry at a time for stream,
###   and for each of those entries we do the math for it
###
######

dict_user_to_friend_group = {} # This is a dictionary of the friend groups of degree CONST_D.
                                # Keys are Center User, Values are friends up to degree CONST_D

dict_friend_to_user_group = {} # This is a dictionary of the friend groups as well, however,
                                # now, given a user (key) the values are each of the groups that that user is in
                                
# IT IS IMPORTANT TO NOTE: These two dictionaries will be the same (because friendships go both ways)
dict_friend_to_user_group = dict_user_to_friend_group # (assignment done symbolically here - doesn't do anything)

def update_friend_groups():
    global dict_user_to_friend
    global dict_user_to_friend_group
    # for each user
    for user in dict_user_to_friend.iterkeys():
        dict_user_to_friend_group.setdefault(user,[])
        friend_list = get_friends(user)
        visited_users = [user]
        curDegree = 1
        # create a friends group, go while there are still friends to explore
        while len(friend_list) > 0:
            # loop through for each friend of the user
            for friend in list(friend_list): # important to recast as a list so we use a list here that won't be modified
                # if its a friend we've already visited, skip to next friend, (and remove from the list as below)
                if visited_users.count(friend) > 0:
                    friend_list.remove(friend)
                    continue
                # IMPORTANT: add the user to the friend group
                dict_user_to_friend_group[user].append(friend)
                # if we're not at the max degrees then go one level deeper, by adding the friends of this friend to the friend_list
                if curDegree < CONST_D:
                    friend_list.extend(get_friends(friend))
                # remove this friend from the friend_list - we started with a friend list of the user, and the next loop step will
                #   have all of the friends of friends 1 degree away (next loop with be degree 2 etc)
                friend_list.remove(friend)
                visited_users.append(friend)
            curDegree += 1

update_friend_groups()

print ('')
print (dict_user_to_friend_group)
print ('')

# AGAIN, IT IS IMPORTANT TO NOTE: These two dictionaries will be the same (because friendships go both ways)
dict_friend_to_user_group = dict_user_to_friend_group # this one actually does something as compared to above

## Now, we have a dictionary of the users that are D degrees away from each other: dict_user_to_friend_group

## The next step is to read in the Batch and create a history for each friend group of the last CONST_T purchases
dict_friend_group_purchase_history = {}
def read_in_purchases(data):
    global dict_friend_group_purchase_history
    global dict_user_to_friend_group
    for entry in data:
        id1 = int(entry['id'])
        amount = float(entry['amount'])
        for user in dict_user_to_friend_group[id1]:
            dict_friend_group_purchase_history.setdefault(user,[])
            dict_friend_group_purchase_history[user].append(amount)
            dict_friend_group_purchase_history[user] = dict_friend_group_purchase_history[user][-CONST_T:]
    print (dict_friend_group_purchase_history)

read_in_purchases(data_batch_P)
print ('')

# create an empty output file
# from: https://stackoverflow.com/questions/12654772/create-empty-file-using-python
open(sys.argv[3], 'w').close()

def read_in_purchases_and_check_for_anomolies(data):
    global dict_friend_group_purchase_history
    global dict_user_to_friend_group
    global dict_user_to_friend
    for entry in data:
        if entry['event_type'] == 'purchase':
            user = int(entry['id'])
            amount = float(entry['amount'])
            # for each friend of that user that just purchased, go through the friends in their friend group
            for friend in dict_user_to_friend_group[user]:
                print (id1, friend, user, id1)
                dict_friend_group_purchase_history.setdefault(friend,[])

                # get that friend's purchase history and calc the mean and SD
                tmpList = dict_friend_group_purchase_history[friend]
                
                if len(tmpList) > 0:
                    data_mean = calc_mean(tmpList)
                    data_SD = calc_SD(tmpList, data_mean)

                    if data_SD != -1 and amount > data_mean + data_SD * 3:
                        with open(sys.argv[3], 'a') as outfile:
                            ## Force the order with an ordered dictionary - just passing the dictionary gives wrong order
                            tmpOut_dict = OrderedDict([('event_type',entry['event_type']), ('timestamp',entry['timestamp']), ('id',entry['id']), ('amount',entry['amount']), ('mean',str("{0:.2f}".format(data_mean))), ('sd',str("{0:.2f}".format(data_SD)))])
                            json.dump(tmpOut_dict, outfile)
                            break
                                
                dict_friend_group_purchase_history[friend].append(amount)
                dict_friend_group_purchase_history[friend] = dict_friend_group_purchase_history[friend][-CONST_T:]


        elif entry['event_type'] == 'befriend':
            # make sure the users exist
            dict_user_to_friend.setdefault(id1,[])
            dict_user_to_friend.setdefault(id2,[])
            # add them to the dictionary
            dict_user_to_friend[id1].append(id2)
            dict_user_to_friend[id2].append(id1)
            update_friend_groups()
            
        elif entry['event_type'] == 'unfriend':
            # make sure the users exist
            dict_user_to_friend.setdefault(id1,[])
            dict_user_to_friend.setdefault(id2,[])
            # remove them from the dictionary
            dict_user_to_friend[id1].remove(id2)
            dict_user_to_friend[id2].remove(id1)
            update_friend_groups()

    print (dict_friend_group_purchase_history)



# Read in the Stream JSON file
data_stream_P = []
data_stream_L1 = {}
data_stream_F = []
data_stream = []
with open(sys.argv[2]) as json_in_file:
    for entry in json_in_file:
        data_stream.append(json.loads(entry))

read_in_purchases_and_check_for_anomolies(data_stream)

##
##purchase_list = {}
##def recalculate_purchase_lists():
##    global purchase_list
##    global dict_user_to_friend
##    global CONST_T
##    global CONST_D
##    
##    # for each user
##    for user in dict_user_to_friend.iterkeys():
##        purchase_list[user] = {}
##        friend_list = get_friends(user)
##        visited_users = []
##        curDegree = 1
##        # create a friends group
##        while len(friend_list) > 0:
##            # loop through for each friend of the user
##            for friend in friend_list:
##                # if its a friend we've already visited, skip to next friend, (and remove from the list as below)
##                if visited_users.count(friend) > 0:
##                    friend_list.remove(friend)
##                    continue
##                # loop through all of the purchases of that friend
##                for each_purchase in get_purchases(friend):
##                    ## HERE is where we have to add the purchases WITH the purchase date
##                    #  so that we can order them (order purchase_list) by purchase date
##                    #  and keep only the 50 most recent
##                    purchase_list[user][:].append(each_purchase)
##                    print (purchase_list[user])
##                    ## HERE we keep only the 50 most recent
##                    purchase_list[user] = purchase_list[user][-CONST_T:]
##                # if we're not at the max degrees then go one level deeper, by adding the friends of this friend to the friend_list
##                if curDegree < CONST_D:
##                    friend_list.extend(get_friends(friend))
##                # remove this friend from the friend_list - we started with a friend list of the user, and the next loop step will
##                #   have all of the friends of friends 1 degree away (next loop with be degree 3 etc
##                friend_list.remove(friend)
##                visited_users.append(friend)
##            curDegree += 1
##
### update the list of purchases per each friend group (AKA do that code right above)
##recalculate_purchase_lists()

#################################################################################################################################
# Above code created a purchase_list dictionary, where the key is the user
#  and the value is a list of purchases calculated for the friend group of degree D
#
# Below will read in the Stream file and look for anomolies
######

##def calc_outstanding_purchases_with_amount(user, amount):
##    ## now that we have purchase lists, we can calc the means and SD
##    if len(purchase_list) > 0:
##        data_mean = calc_mean(purchase_list[user])
##        data_SD = calc_SD(purchase_list[user], data_mean)
##        if data_SD == -1:
##            return -1
##        if amount > data_mean + data_SD * 3:
##            return [amount, data_mean, data_SD]
##    return -1
##
##def calc_outstanding_purchases(user):
##    ## now that we have purchase lists, we can calc the means and SD
##    if len(purchase_list) > 0:
##        data_mean = calc_mean(purchase_list[user])
##        data_SD = calc_SD(purchase_list[user], data_mean)
##        if data_SD == -1:
##            return -1
##        for entry in purchase_list:
##            if entry > data_mean + data_SD * 3:
##                return [entry, data_mean, data_SD]
##    return -1


### start reading the steam file
##for entry in data_stream:
##    if 'event_type' in entry:
##        if entry.get('event_type') == 'purchase': ## equivalent lines: "entry['event_type']" and "entry.get('event_type')" 
##            id1 = int(entry['id'])
##            dict_user_to_purchase.setdefault(id1,[])
##            dict_user_to_friend.setdefault(id1,[])
##
##            [a,b,c] = calc_outstanding_purchases_with_amount(id1, float(entry['amount']))
##            if a != -1: # -1 if not anomoly, otherwise it returns the amount
##                with open(sys.argv[3], 'a') as outfile:
##                    ## Force the order with an ordered dictionary - just passing the dictionary gives wrong order
##                    tmpOut_dict = OrderedDict([('event_type',entry.get('event_type')), ('timestamp',entry.get('timestamp')), ('id',entry.get('id')), ('amount',entry.get('amount')), ('mean',str("{0:.2f}".format(b))), ('sd',str("{0:.2f}".format(c)))])
##                    json.dump(tmpOut_dict, outfile)
##            
##            dict_user_to_purchase[id1].append(float(entry['amount']))
##            data_batch_P.append(entry)
##            
##            # recalculate the purchase lists for each friend group - two purchases could come in and we have to recalculate for each
##            # this is below as we don't want the new purchase to affect the mean, sd
##            recalculate_purchase_lists()
##                    
##            
##        elif entry['event_type'] == 'befriend':
##            # first make sure the users exist
##            dict_user_to_friend.setdefault(id1,[])
##            dict_user_to_friend.setdefault(id2,[])
##            dict_user_to_purchase.setdefault(id1,[])
##            dict_user_to_purchase.setdefault(id2,[])
##            # make friend group modifications
##            dict_user_to_friend[id1].append(id2)
##            dict_user_to_friend[id2].append(id1)
##            data_batch_F.append(entry)
##        
##            # recalculate the purchase lists for each friend group - two purchases could come in and we have to recalculate for each
##            recalculate_purchase_lists()
##            
##            [a,b,c] = calc_outstanding_purchases(id1)
##            if a != -1: # -1 if not anomoly, otherwise it returns the entry
##                with open(sys.argv[3], 'a') as outfile:
##                    ## Force the order with an ordered dictionary - just passing the dictionary gives wrong order
##                    tmpOut_dict = OrderedDict([('event_type',entry.get('event_type')), ('timestamp',entry.get('timestamp')), ('id',entry.get('id')), ('amount',entry.get('amount')), ('mean',str("{0:.2f}".format(b))), ('sd',str("{0:.2f}".format(c)))])
##                    json.dump(entry, outfile)
##                    
##            [a,b,c] = calc_outstanding_purchases(id2)
##            if a != -1: # -1 if not anomoly, otherwise it returns the entry
##                with open(sys.argv[3], 'a') as outfile:
##                    ## Force the order with an ordered dictionary - just passing the dictionary gives wrong order
##                    tmpOut_dict = OrderedDict([('event_type',entry.get('event_type')), ('timestamp',entry.get('timestamp')), ('id',entry.get('id')), ('amount',entry.get('amount')), ('mean',str("{0:.2f}".format(b))), ('sd',str("{0:.2f}".format(c)))])
##                    json.dump(entry, outfile)
##
##        elif entry['event_type'] == 'unfriend':
##            # make friend group modifications
##            try:
##                dict_user_to_friend[id1].remove(id2)
##                dict_user_to_friend[id2].remove(id1)
##                data_batch_F.append(entry)
##            except:
##                print ('Dangnabbit! Yer stream file tried to remove a user not in the database! (process_log.py line ~365)')
##                
##            # recalculate the purchase lists for each friend group - two purchases could come in and we have to recalculate for each
##            recalculate_purchase_lists()
##            [a,b,c] = calc_outstanding_purchases(id1)
##            if a != -1: # -1 if not anomoly, otherwise it returns the entry
##                with open(sys.argv[3], 'a') as outfile:
##                    ## Force the order with an ordered dictionary - just passing the dictionary gives wrong order
##                    tmpOut_dict = OrderedDict([('event_type',entry.get('event_type')), ('timestamp',entry.get('timestamp')), ('id',entry.get('id')), ('amount',entry.get('amount')), ('mean',str("{0:.2f}".format(b))), ('sd',str("{0:.2f}".format(c)))])
##                    json.dump(entry, outfile)
##            [a,b,c] = calc_outstanding_purchases(id2)
##            if a != -1: # -1 if not anomoly, otherwise it returns the entry
##                with open(sys.argv[3], 'a') as outfile:
##                    ## Force the order with an ordered dictionary - just passing the dictionary gives wrong order
##                    tmpOut_dict = OrderedDict([('event_type',entry.get('event_type')), ('timestamp',entry.get('timestamp')), ('id',entry.get('id')), ('amount',entry.get('amount')), ('mean',str("{0:.2f}".format(b))), ('sd',str("{0:.2f}".format(c)))])
##                    json.dump(entry, outfile)
##            
##    elif 'T' in entry:
##        data_batch_L1.update(entry)

