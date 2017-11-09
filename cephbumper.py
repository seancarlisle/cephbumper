#!/bin/python2.7

import sys
import os
import json
import subprocess
import re
from datetime import datetime

desired_weight=3.63689
desired_pgp=8192

# Basic logging function for keeping track of what the script is doing (or trying to do)
def logging(message):
   logHandle = open('/root/cephbumper.log','a')

   messageLines = message.split('\n')
   formattedMessage = ''
   # Append the timestamp to each line to make the logging more "official"
   for line in messageLines:
      formattedMessage += str(datetime.utcnow().isoformat(' '))[:-3] + " " + line + "\n"

   logHandle.write(formattedMessage)
   logHandle.flush()
   logHandle.close()

###################### MAIN LOGIC ###############################

# If the cluster is in error state, or in warning because of rebalancing, abort
cluster_health = subprocess.check_output(['ceph','health']).rstrip('\n')
if 'HEALTH_ERR' in cluster_health:
   logging("Cluster health in Error state: %s Exiting" %(cluster_health))
   exit(1)
elif "HEALTH_WARN" in cluster_health:
    if "backfill" in cluster_health or "stuck" in cluster_health or "unclean" in cluster_health:
       logging("Cluster health in Warn state: %s Exiting" %(cluster_health))
       exit(1)

# Cluster is ready for additional rebalancing.

# Get the crush map into json format
crush_map_json=json.loads(subprocess.check_output(['ceph','osd','crush','tree']))

# Here comes the fun part! So many layers just to get to the OSDs!  There must be an easier way to do this...
# I had originally been pulling in the crush map and doing things with it, but its far easier to simply
# receive a list of OSD and target them for weight increase.
   for osd in osds:
      #print str(subprocess.check_output(['ceph','osd','tree','|','grep',str(osd['id'])]))
      # If we haven't hit our desired weight yet, increase by 0.1 or desired_weight - crush_weight if the
      # difference is smaller than 0.1
      if round(float(desired_weight),5) != round(float(osd['crush_weight']),5):
         #print "Here I would increase the weight by 0.1 until desired weight of %f" %(desired_weight)
         reweight_amount = 0.15
         if desired_weight - float(osd['crush_weight']) < reweight_amount:
            reweight_amount = round(desired_weight - float(osd['crush_weight']),5)
         logging("Increasing weight of %s by %f" %(str(osd['id']), reweight_amount))
         increase_to = round(reweight_amount + float(osd['crush_weight']),5)
         logging("Current increase to: %s" %(str(increase_to)))
         ##### DANGER ####
         subprocess.check_output(['ceph','osd','crush','reweight', "osd."+str(osd['id']), str(increase_to)])
         ##### DANGER ####
      else:
         logging("Reached desired weight. Nothing to do.")


###################### OLD CODE IGNORE ###############################
                  #if desired_weight - float(osd['crush_weight']) < 0.1:
                  #   reweight_amount = round(desired_weight - float(osd['crush_weight']),5)
                  #   print "Increasing weight of %s by %f" %(str(osd['id']),reweight_amount)
                  #else:
                  #   print "Increasing weight of %s by 0.1" %(str(osd['id']))
                  # subprocess.check_output(['ceph','osd','crush','reweight', "osd."+str(osd['id']), str(reweight_amount))

###################### OLD CODE IGNORE ###############################

# Get the current pgp_num value
current_pgp=str(subprocess.check_output(['ceph','osd','pool','get','volumes-sata','pgp_num']))[9:]
#print "Here I would increase the pgp_num by 100 until desired pgp_num of %d" %(desired_pgp)
print "current pgp: %s" % current_pgp
#pgp_increase = 100
pgp_increase = 150

# Like with the OSD weights, if we haven't hit our desired pgp_num yet, increase by 100
# or desired_pgp - current_pgp if the difference is smaller than 100
if int(desired_pgp) != int(current_pgp):
   if desired_pgp - int(current_pgp) < pgp_increase:
      pgp_increase = desired_pgp - int(current_pgp)
   logging("Increasing pgp_num by %d" %(pgp_increase))
   increase_to = int(current_pgp) + pgp_increase
   logging("Current increase to: %s" %(str(increase_to)))
   ##### DANGER ####
   subprocess.check_output(['ceph','osd','pool','set','volumes-sata','pgp_num',str(increase_to)])
   ##### DANGER ####
else:
   logging("Reached desired pgp_num. Nothing to do")


###################### OLD CODE IGNORE ###############################
   # Increase the pgp_num
   #current_pgp=str(subprocess.check_output(['ceph','osd','pool','get','volumes-sata','pgp_num']))[9:]
   #print "Here I would increase the pgp_num by 100 until desired pgp_num of 8192"
   #print "current pgp: %s" % current_pgp
   #pgp_increase = 100
   #if desired_pgp - int(current_pgp) < 100:
   #  pgp_increase = desired_pgp - int(current_pgp)
   #  print "Increasing pgp_num by %d" %(pgp_increase)
   #else:
   #  print "Increasing pgp_num by 100"
   # subprocess.check_output(['ceph','osd','pool','set','volumes-sata','pgp_num',str(pgp_increase)])

###################### OLD CODE IGNORE ###############################
