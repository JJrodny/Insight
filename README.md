Jeff Rodny 7/4/17 Insight Data Engineering

The name of the running python file is the same as the original python file, that way run.sh can be identical - sorry for not being creative!

The python required:
import json
import sys
import math
from collections import OrderedDict (to make sure the JSON outputted in the proper order as dictionaries are unordered)

Comments from code:


 Code by Jeff Rodny 7/4/17
 for Insight Data Engineering Code Challenge

 The code makes the following assumptions:
 
 - ASSUMES a pre-sorted (based on date) Batch and Stream file. Sorting is costly here, esp. for large data.
 
 - ASSUMES we check for anomolies ONLY when a user makes a PURCHASE. We can add in the extra code to check on befriend and unfriend events, (which is not a lot), but assuming we want to maximize speed (and the specification document assumes checking for anomolies 'on purchase'), so we don't check on befriend and unfriend events
 
 - ASSUMES when a friend is added we don't update the purchase_history so the past purchases of the new friend are taken into account, just the new purchases that the new friend makes (This is for speed and convenience). It can be made so when a friend is made we retroactively go back and look through the new friends' old purchases but they are ignored for speed (
 
 - ASSUMES User A's social network includes user A:
   There is a contradiction (or clarification is needed) in the document specification:
       "T: the number of consecutive purchases made by a user's social network (not including the user's own purchases)"
       "For example, if D = 1, User A's social network would only consist of User B and User C"
       "If D = 2, User A's social network would consist of User B, User C, and User D."
       "A purchase amount is anomalous if it's more than 3 standard deviations from the mean of the last T purchases in the user's Dth degree social network."
   Which implies:
   1. User A's social network does not include user A
   2. The average of User A's social network purchases doesn't include user A's purchases
   3. However, the example answer assumes user A's purchases ARE included in user A's social network purchases when B makes a large purchase, or when user B makes a large purchase that purchase IS included in user B's social network purchases (in other words B is in B's social network)

   4. OR it could be, that it's meant that B's purchases are not included in B's social-network-purchase-average, but when B makes a purchase, it could be that we still check if it's purchase is an anomoly in its own social network before recording it and removing it from B's social-network-purchase-average