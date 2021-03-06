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
# ASSUMES User A's social network includes user A:
#   There is a contradiction (or clarification is needed) in the document specification:
#       "T: the number of consecutive purchases made by a user's social network (not including the user's own purchases)"
#       "For example, if D = 1, User A's social network would only consist of User B and User C"
#       "If D = 2, User A's social network would consist of User B, User C, and User D."
#       "A purchase amount is anomalous if it's more than 3 standard deviations from the mean of the last T purchases in the user's Dth degree social network."
#   Which implies:
#   1. User A's social network does not include user A
#   2. The average of User A's social network purchases doesn't include user A's purchases
#   3. However, the example answer assumes user A's purchases ARE included in user A's social network purchases when B makes a large purchase,
#      or when user B makes a large purchase that purchase IS included in user B's social network purchases (in other words B is in B's social network)
#
#   4. OR it could be, that it's meant that B's purchases are not included in B's social-network-purchase-average,
#       but when B makes a purchase, it could be that we still check if it's purchase is an anomoly
#       in its own social network before recording it and removing it from B's social-network-purchase-average
###

import json
import sys
import math
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

# Create a dictionary where key is userID, and each value is a linked list of 1st degree friends
dict_user_to_friend = {}
with open(sys.argv[1]) as json_in_file:
    for line in json_in_file:
        entry = json.loads(line)
##        print (entry)
        
        # save D and T values
        if 'T' in entry:
            CONST_T = int(entry['T'])
            CONST_D = int(entry['D'])
        # Populate that dictionary where key is userID, and each value is a linked list of 1st degree friends
        elif entry['event_type'] == 'befriend':
            id1 = int(entry['id1'])
            id2 = int(entry['id2'])
            # make sure the users exist
            dict_user_to_friend.setdefault(id1,[])
            dict_user_to_friend.setdefault(id2,[])
##            dict_user_to_friend.setdefault(id1,[id1]) ## These lines are commented out so that user B is not in user B's social network
##            dict_user_to_friend.setdefault(id2,[id2])
            # add them to the dictionary
            dict_user_to_friend[id1].append(id2)
            dict_user_to_friend[id2].append(id1)
            
        # remove the friendships that you read in with 'unfriend'
        elif entry['event_type'] == 'unfriend':
            id1 = int(entry['id1'])
            id2 = int(entry['id2'])
            # remove them from the dictionary
            try:
                dict_user_to_friend[id1].remove(id2)
                dict_user_to_friend[id2].remove(id1)
            except:
                print ('Dangnabbit! Yer Batch file tried to remove a user not in the database! (process_log.py line ~90)')

        # save purchases for later (need a built up dict_user_to_friend first)
        elif entry['event_type'] == 'purchase':
            data_batch_P.append(entry)
            


print ('dict_user_to_friend', dict_user_to_friend)

# get the friends of a user - unnecessary but looks nice
def get_friends(userID):
    return list(dict_user_to_friend[userID])

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
# dict_friend_to_user_group = dict_user_to_friend_group # (assignment done symbolically here - doesn't do anything)

# update friend groups - populate dict_user_to_friend_group
#   this is like the previous dict_user_to_friend, but instead of Key:User, Value:1st degree friends
#   it is Key:User, Value:Dth degree friends, and this code is called multiple times (when a befriend
#   or unfriend is read in stream)
def update_friend_groups():
    global dict_user_to_friend
    global dict_user_to_friend_group
    # for each user
    for user in dict_user_to_friend.iterkeys():
        dict_user_to_friend_group.setdefault(user,[])
        friend_list = get_friends(user)
##        visited_users = [user] ## This line is commented out so that B is included in B's social-network-purchase-average
        visited_users = []
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
print ('dict_user_to_friend_group', dict_user_to_friend_group)
print ('')

# AGAIN, IT IS IMPORTANT TO NOTE: These two dictionaries will be the same (because friendships go both ways)
# dict_friend_to_user_group = dict_user_to_friend_group # this one would actually do something here as compared to above

## Now, we have a dictionary of the users that are D degrees away from each other: dict_user_to_friend_group

## The next step is to read in the Batch and create a history for each friend group of the last CONST_T purchases
dict_friend_group_purchase_history = {}
for entry in data_batch_P:
    id1 = int(entry['id'])
    amount = float(entry['amount'])
    for user in dict_user_to_friend_group[id1]:
        dict_friend_group_purchase_history.setdefault(user,[])
        dict_friend_group_purchase_history[user].append(amount)
        dict_friend_group_purchase_history[user] = dict_friend_group_purchase_history[user][-CONST_T:]
##print (dict_friend_group_purchase_history)
##print ('')

# create an empty output file
# from: https://stackoverflow.com/questions/12654772/create-empty-file-using-python
open(sys.argv[3], 'w').close()

# Read in the Stream JSON file
data_stream_P = []
data_stream_L1 = {}
data_stream_F = []
data_stream = []
with open(sys.argv[2]) as json_in_file:
    for line in json_in_file:
        entry = json.loads(line)
        if entry['event_type'] == 'purchase':
            user = int(entry['id'])
            amount = float(entry['amount'])
            # for each friend of that user that just purchased, go through the friends in their friend group
            for friend in dict_user_to_friend_group[user]:
                dict_friend_group_purchase_history.setdefault(friend,[])

                # get that friend's purchase history and calc the mean and SD
                tmpList = dict_friend_group_purchase_history[friend]
##                print (tmpList)
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
        
