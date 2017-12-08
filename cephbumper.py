#!/bin/python2.7

import sys
import os
import json
import subprocess
import re
from datetime import datetime
import argparse

#desired_weight=3.63689
#desired_pgp=8192

class ceph_bumper:
   def __init__(self, desired_weight, desired_pgp=0, reweight_amount, pgp_amount=0, debug=False, dry_run=False, osd_list):
      self.desired_weight = desired_weight
      self.reweight_amount = reweight_amount
      self.debug = debug
      self.dry_run = dry_run
      self.osd_list = osd_list
      self.crush_map = ''
      # sean4574: not ready to implement pgp increase yet #
      # self.desired_pgp = desired_pgp
      # self.pgp_amount = pgp_amount
      #self.pool_name = pool_name
      # sean4574: not ready to implement pgp increase yet #
      try:
         self.crush_map = json.loads(subprocess.check_output(['ceph','osd','crush','tree'])
      except Exception as e:
         _logging(str(e))
   
   # Basic logging function for keeping track of what the script is doing (or trying to do)
   def _logging(self,message):
      logHandle = open('/root/cephbumper.log','a')

      messageLines = message.split('\n')
      formattedMessage = ''
      # Append the timestamp to each line to make the logging more "official"
      for line in messageLines:
         formattedMessage += str(datetime.utcnow().isoformat(' '))[:-3] + " " + line + "\n"

      logHandle.write(formattedMessage)
      logHandle.flush()
      logHandle.close()

   # Get cluster health
   def _cluster_health(self):
      try:
         return subprocess.check_output(['ceph','health']).rstrip('\n')
      except e as Exception:
         _logging(str(e))

   # Increase OSD Weight
   #def _osd_weight_increase(self, osd_id, increase_amt):
   #   try:
   #      return subprocess.check_output(['ceph','osd','crush','reweight', "osd."+str(osd_id), str(increase_amt)])
   #   except e as Exception:
   #      _logging(str(e))

   # sean4574: not ready to implement pgp increase yet #
   # Increase pgp Number
   #def _pgp_increase(self, increase_amt):
   #   try:
   #      return subprocess.check_output(['ceph','osd','pool','set','volumes-sata','pgp_num',str(increase_amt)])
   #   except e as Exception:
   #     _logging(str(e))
   # sean4574: not ready to implement pgp increase yet #

   # Retrieve Current OSD Weight
   def _get_osd_weight(self, osd):
      # Find the OSD in the tree
      # sean4574 I initially thought about simply using "ceph osd tree" to get the weight and status,
      # but from a screen-scraping perspective its terrible. Inconsistent spaces and other nastiness...
      # It actually seemed easier to do a binary tree traversal...crazy.
      
      # Get the osd location
      location = {}
      try:
         location_str = subprocess.check_output(['ceph','osd','find',str(osd)])
      except Exception as e:
         _logging(str(e))
      
      crush_location = json.loads(location_str)['crush_location']

      # Find the OSD
      osd_root = None
      osd_rack = None
      osd_host = None
      for root in self.crush_map:
         if root['name'] == crush_location['root']:
            osd_root = root
            if 'rack' in crush_location:
               for rack in osd_root:
                  if rack['name'] == crush_location['rack']:
                     osd_rack = rack
                  for host in osd_rack:
                     if host['name'] == crush_location['host']:
                        osd_host = host
            else:
               for host in osd_rack:
                  if host['name'] == crush_location['host']:
                     osd_host = host
            for osd in osd_host:
               if osd['id'] == int(osd):
                  return osd['crush_weight']

   # Verify if cluster is healthy
   def _verify_cluster_health(self):
      health = _cluster_health()
      proceed = True
      if health is not None:
         if "HEALTH_ERR" in health:
            proceed = False
            _logging("Cluster health in Error state: %s Investigate." %(health))
         elif "HEALTH_WARN" in health:
            if "backfill" in cluster_health or "stuck" in cluster_health or "unclean" in cluster_health:
               _logging("Cluster health in Warn state: %s Exiting" %(cluster_health))
               proceed = False
      return proceed

        
   # Increase OSD weight
   def osd_weight_increase(self, osd_list, desired_weight, increase_amt):
      proceed = _verify_cluster_health()
      assert proceed == True

      for osd in osd_list:
         osd_weight = round(float(_get_osd_weight(osd)),5)
         if osd_weight < desired_weight:
            increase_amt = self.reweight_amt
            if round(float(desired_weight - osd_weight),5) < increase_amt:
               increase_amt = round(float(desired_weight - osd_weight),5)
            self._logging("Increasing weight of %s by %f" %(osd, increase_amt))
            try:
               subprocess.check_output(['ceph','osd','crush','reweight',str(increase_amt)])
            except Exception as e:
               self._logging(str(e))
         else:
            self._logging("OSD %s already at desired weight. Nothing to do." %(osd))

   # TODO: sean4574 implement method for increasing pg and pgp_num

 
def main():
   parser = argparse.ArgumentParser()
   parser.add_argument('--desired-weight', type=float,help='Desired value for weight of an OSD', required=False, dest='desired_weight')
   parser.add_argument('--reweight-amount', type=float,help='Amount to increase the weight for each run.', required=False, dest='reweight_amt')
   parser.add_argument('--debug', type=bool,help='Turn on debug output. Defaults to False.', required=False, dest='debug')
   parser.add_argument('--dry-run', type=bool,help='Simulate changes only.', required=False, dest='dry_run')
   parser.add_argument('--osd-list', type=list, help='List of OSD to have their weight increased.', required=True, dest='osd_list', nargs=argparse.REMAINDER)

   # sean4574: not ready to implement pgp increase yet #
   # parser.add_argument('--desired-pgp', type=int,help='Desired value for final pgp value', required=False, dest='desired_pgp')
   # parser.add_argument('--pgp-amount', type=int,help='Amount to increase pgp_num per run.', required=False, dest='pgp_amount')
   # parser.add_argument('--pool-name',help='Name of pool for pg increase', required=False, dest='pool_name')
   # sean4574: not ready to implement pgp increase yet #   
   args = parser.parse_args()
   #cb = ceph_bumper(args.desired_weight, args.desired_pgp, args.reweight_amount, args.pgp_amount, args.debug=False, args.dry_run=False, args.osd_list)
   cb = ceph_bumper(args.debug, args.dry_run)
   cb.osd_weight_increase(args.osd_list, args.desired_weight, args.reweight_amount)


      #self.desired_weight = desired_weight
      #self.reweight_amount = reweight_amount
      #self.debug = debug
      #self.dry_run = dry_run
      #self.osd_list = osd_list
      #self.pool_name = pool_name
      # sean4574: not ready to implement pgp increase yet #
      #self.pgp_amount = pgp_amount
      #self.desired_pgp = desired_pgp
      # sean4574: not ready to implement pgp increase yet #
###################### MAIN LOGIC ###############################

# If the cluster is in error state, or in warning because of rebalancing, abort
#cluster_health = subprocess.check_output(['ceph','health']).rstrip('\n')
#if 'HEALTH_ERR' in cluster_health:
#   logging("Cluster health in Error state: %s Exiting" %(cluster_health))
#   exit(1)
#elif "HEALTH_WARN" in cluster_health:
#    if "backfill" in cluster_health or "stuck" in cluster_health or "unclean" in cluster_health:
#       logging("Cluster health in Warn state: %s Exiting" %(cluster_health))
#       exit(1)

# Cluster is ready for additional rebalancing.

# Get the crush map into json format
#crush_map_json=json.loads(subprocess.check_output(['ceph','osd','crush','tree']))

# Here comes the fun part! So many layers just to get to the OSDs!  There must be an easier way to do this...
# I had originally been pulling in the crush map and doing things with it, but its far easier to simply
# receive a list of OSD and target them for weight increase.
#   for osd in osds:
      #print str(subprocess.check_output(['ceph','osd','tree','|','grep',str(osd['id'])]))
      # If we haven't hit our desired weight yet, increase by 0.1 or desired_weight - crush_weight if the
      # difference is smaller than 0.1
#      if round(float(desired_weight),5) != round(float(osd['crush_weight']),5):
#         #print "Here I would increase the weight by 0.1 until desired weight of %f" %(desired_weight)
#         reweight_amount = 0.15
#         if desired_weight - float(osd['crush_weight']) < reweight_amount:
#            reweight_amount = round(desired_weight - float(osd['crush_weight']),5)
#         logging("Increasing weight of %s by %f" %(str(osd['id']), reweight_amount))
#         increase_to = round(reweight_amount + float(osd['crush_weight']),5)
#         logging("Current increase to: %s" %(str(increase_to)))
#         ##### DANGER ####
#         subprocess.check_output(['ceph','osd','crush','reweight', "osd."+str(osd['id']), str(increase_to)])
#         ##### DANGER ####
#      else:
#         logging("Reached desired weight. Nothing to do.")


# Get the current pgp_num value
#current_pgp=str(subprocess.check_output(['ceph','osd','pool','get','volumes-sata','pgp_num']))[9:]
#print "Here I would increase the pgp_num by 100 until desired pgp_num of %d" %(desired_pgp)
#print "current pgp: %s" % current_pgp
#pgp_increase = 100
#pgp_increase = 150

# Like with the OSD weights, if we haven't hit our desired pgp_num yet, increase by 100
# or desired_pgp - current_pgp if the difference is smaller than 100
#if int(desired_pgp) != int(current_pgp):
#   if desired_pgp - int(current_pgp) < pgp_increase:
#      pgp_increase = desired_pgp - int(current_pgp)
#   logging("Increasing pgp_num by %d" %(pgp_increase))
#   increase_to = int(current_pgp) + pgp_increase
#   logging("Current increase to: %s" %(str(increase_to)))
#   ##### DANGER ####
#   subprocess.check_output(['ceph','osd','pool','set','volumes-sata','pgp_num',str(increase_to)])
#   ##### DANGER ####
#else:
#   logging("Reached desired pgp_num. Nothing to do")


#def main():
#   ar
