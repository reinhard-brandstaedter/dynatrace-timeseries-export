#! /usr/bin/env python
#
# Dynatrace Managed Timeseries Exporter
# (c) 2018 Reinhard Brandstaedter @ SAP CX 
# 
# infraexport fetches infrastructure information from Dynatrace and pushes them into a timeseriese database (influxDB)
#
# Features:
#
# - the infraexport doesn't really fetch information from Dynatrace but uses the shared redis cache where
#   the metricexport already stores entity information and reuses this data
# - data is pulled every 10 minutes (hardcoded) and pusehd to influxdb with certain host tags
#
# Warnings:
# - for large environments there can be a significant impact on the Dynatrace API.
#   We are using this setup in environments with 6k+ hosts and 40k processes


from __future__ import print_function
from random import randint
import requests, json
import redis
import os, sys, getopt
import datetime, time, random
import urllib3
import logging
import traceback


# set configuration from environment variables
apitoken = os.environ['DT_API_TOKEN']
tenantid = os.environ['DT_API_TENANT']
server = os.environ['DT_API_SERVER']
influxdb =  os.environ['INFLUXDB_URL']
influxuser = os.environ['INFLUXDB_USER']
influxpass = os.environ['INFLUXDB_PASS']
# separate ports are required for crappy influxDB Relay, create DB and retention policies have to be done via readport :-) 
influxdb_write_port = os.environ['INFLUXDB_WRITE_PORT']
influxdb_read_port = os.environ['INFLUXDB_READ_PORT']
tokenduration = int(os.environ['DT_TOKEN_DURATION'])

#influxdbquery = os.environ['INFLUXDB_URL'] + '/query?'
influxdbs = []

# LOG CONFIGURATION
FORMAT = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=FORMAT)
logger = logging.getLogger("infraexport")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

influxdb_batch_size = 8000
fetchinterval = 600 #delay in seconds between fetches

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

entitycache = redis.StrictRedis(host='entitycache', port=6379, db=0)


def influxdbpost(session,db,data):

    for influxhost in influxdbs:
        if influxhost == 'dummy':
            continue
        try:
            #influxwrite = influxhost + '/write?db=' + str(db)
            influxwrite = influxhost  + ':' + influxdb_write_port +  '/write?db=' + str(db) + '&u=' + influxuser + '&p=' + influxpass
            #logger.info(influxwrite)
            resp = session.post(influxwrite, data=data, timeout=20)
            if 200 <= resp.status_code <= 204:
                continue
            else: 
                logger.error('InfluxDB post not OK: {} {}'.format(resp.status_code,influxwrite))
        except:
            logger.error('{} \n {}'.format(influxdb+str(db),data))
            logger.error(traceback.format_exc())


# the entitycache should contain all hosts and their information so we can use the cache that is populated by the timeseries export to get all required information
def getCachedHosts(tenant):
    cnt = 0
    postsession = requests.Session()
    postsession.trust_env = False # disable any environments set proxy
    
    for entityID in entitycache.scan_iter("HOST*"):
        citem = entitycache.get(entityID)
        tags = {}
        fields = {}
        if citem is not None:
            host = json.loads(citem)[0]
            #logger.info('Cached Host: {}'.format(host))
            try:
                for tag in host['tags']:
                    if 'key' in tag and 'value' in tag:
                        tags.update({tag['key'].lower():tag['value']})
                        
                tags.update({'host':host['displayName']})
                tags.update({'ostype':host['osType']})
                tags.update({'osversion':host['osVersion']})
                #tags.update({'monitoringmode':host['monitoringMode']})
                if 'agentversion' in host:
                    tags.update({'agentversion':'{}.{}'.format(host['agentVersion']['major'],host['agentVersion']['minor'])})
            except:
                logger.warning('Problem with tags on ' + host['displayName'])
                logger.warning('Host tag record: {}'.format(tags))
                
            try:                
                fields.update({'cpucores':host['cpuCores']})
                fields.update({'hostunits':host['consumedHostUnits']})
            except:
                logger.warning('Problem with fields on ' + host['displayName'])
                
            line = 'infra'
            tagstr = ','.join('{}={}'.format(key, value.replace(' ','\ ').replace(',','\,')) for key,value in tags.items())
            fieldstr = ','.join('{}={}'.format(key, value) for key,value in fields.items())
            
            #for infrastructure information the timestamp is irrelevanat, so simply use current time
            line += ',' + tagstr + ' ' + fieldstr + ' ' + str(int(round(time.time() * 1000) *1000000))
            
            try:
                influxdbpost(postsession,tags['client'],line)
                # always post a copy to the BI database "HCS"
                influxdbpost(postsession,'hcs',line)
            except:
                logger.error('Posting to InfluxDB failed: {}'.format(host['displayName']))
                logger.error(traceback.format_exc())
            
        cnt += 1
    
    postsession.close()    
    logger.info("Hosts: {} Tenant: {}".format(cnt,tenant))
    
def main(argv):
    # for simultaneous post to multiple InfluxDBs
    # if env variable INFLUXDB_URL is a list
    global influxdbs
    influxdbs = [db.strip() for db in influxdb.split(',')]
    
    logger.info('Configuration: {} {} --> {}'.format(server, apitoken, influxdbs))
    logger.info('Check-Interval: {}'.format(fetchinterval))
    
    while True:
        getCachedHosts("all tenants")
        time.sleep(fetchinterval)
    
if __name__ == "__main__":
    main(sys.argv[1:])
