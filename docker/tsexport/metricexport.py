#! /usr/bin/env python
#
# Dynatrace Managed Timeseries Exporter
# (c) 2018 Reinhard Brandstaedter @ SAP CX 
# 
# metricexport (tsexport) fetches timeseries from Dynatrace and pushes them into a timeseriese database (influxDB)
#
# Features:
#
# - works for all tenants of the Dynatrace cluster, uses a cluster management token (security) to
#   dynamically create dataexport tokens on the individual tenants with timeout
# - uses redis as cache for dynatrace entity infromation (hosts, processes, processgroups, ..) 
#   to avoid unnecessary fetches on the dynatrace API
# - caches meta information (data export tokens, timeseries fetch window, influxDB info) in redis cache
# - creates the InfluxDBs if they do not exist by posting create database statements
#
# Warnings:
# - for large environments there can be a significant impact on the Dynatrace API.
#   We are using this setup in environments with 6k+ hosts and 40k processes
# - carefull with setting the worker processes. Python can easily kill the Dynatrace API with too many
#   simultaneously requests (we are using 20 worker processes)
# 
# ToDo:
# - some code cleanup of unused functionality 
#
# Configuration Parameters (read from environment variables):
#
# DT_API_SERVER=https://xxxxxx.dynatrace-managed.com
# DT_API_TOKEN=<cluster management API token>
# DT_API_TENANT=<not used anymore>
# DT_TOKEN_DURATION=<duration in seconds for the dataexport token of a tenant, set to high enough to avoid frequent recration of tokens: 3600> 
# INFLUXDB_URL=<url of the influxDB post endpoint, e.g. 'http://myinfluxdb.host' use 'dummy' to post to null>
# INFLUXDB_READ_PORT=8086
# INFLUXDB_WRITE_PORT=8086
# INFLUXDB_USER=influxuser
# INFLUXDB_PASS=influxpass

# TS_CHECK_INTERVAL=60            # the minumum timeinterval between checks of the same metric
# TS_BACKLOG_WINDOW=200           # the default backlog window how far back timeseries should be fetched in seconds
# TS_WORKER_THREADS=4             # the number of worker thread working in parallel through the metric list
# TS_INFLUXDB_BATCH_SIZE=8000     # maxiumum batch size (datapoints) to post to influxdb



from __future__ import print_function
from multiprocessing import Process
#from daemon import Daemon
from random import randint
import requests, json
import redis
import os, sys, getopt, socket
import math
import datetime, time, random
import urllib3
import logging
import traceback

# LOG CONFIGURATION
FORMAT = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=FORMAT)
logger = logging.getLogger("metricexport")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)

# create formatter
formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s","%Y-%m-%d %H:%M:%S")
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

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

#influxdbquery = os.environ['INFLUXDB_URL'] + '/query?'
influxdbs = []

influxdb_batch_size = 8000
check_interval = 60             # how often to fetch new timeseries in seconds
backlog_window = 600            # how far back to fetch timeseries in seconds (useful when restarting the process) max is one hour (3600s)
backlog_window_min = 190
backlog_shift = 90              # to avoid zeros in InfluxDB add a shift to the starttime of fetching 
max_fails = 5
worker_threads = 10
backlogfactor = 2.0
cache_expiry = 7200     # Cache expiry time for entities 2hrs
cache_expiry_deviation = 0.25
tenantexpiry = 3600

#worker configuration
check_interval = int(os.environ['TS_CHECK_INTERVAL'])
backlog_window = int(os.environ['TS_BACKLOG_WINDOW'])
worker_threads = int(os.environ['TS_WORKER_THREADS'])
influxdb_batch_size = int(os.environ['TS_INFLUXDB_BATCH_SIZE'])
tokenduration = int(os.environ['DT_TOKEN_DURATION'])

# blacklist of processes we do not want to fetch information for
blacklist = {'OneAgent','puppet','mcollectived','nrpe','sssd_be','ssd_nss','splunkd','bdsrvscand','master','sshd','ssh','pcsd','org.eclipse.equinox.launcher','systemd', 'rpc.statd', 'jmxquery.jar', 'SAP host agent','dtwsagent','dtselfmon','pdnsd','System Collector'}


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

entitycache = redis.StrictRedis(host='entitycache', port=6379, db=0)

# the state of a metric in the queue
# @last_check_ts: timestamp of last check/successful fetch
# @status: is the metric currently processed
# @nr_checks: number of checks of this metric
# @fetches: number of successful fetches/processing of metric
# @fails: number of failed attempts to get metric
#
class MetricState:
    timeseries = ''
    aggregation = ''
    def __init__(self,metric,aggr):
        self.last_check_ts = self.nr_checks = self.fails = self.fetches = 0
        self.fetch_duration = self.post_duration = self.work_duration = 0.0
        self.status = False
        self.aggregation = aggr.strip()
        self.timeseries = metric.strip()
        self.session = requests.Session()
        self.session.trust_env = False
     
    def getLastCheckTS(self):
        return self.last_check_ts
        
    def updateCheckTS(self,ts):
        self.last_check_ts = ts
        
    def updateFetchDuration(self,duration):
        self.fetch_duration = duration
        
    def updateWorkDuration(self,duration):  
        self.work_duration = duration
          
    def updatePostDuration(self,duration):  
        self.post_duration = duration
        
    def getFetchDuration(self):
        return self.fetch_duration
    
    def getWorkDuration(self):
        return self.work_duration
    
    def getPostDuration(self):
        return self.post_duration
            
    def updateFetches(self):
        self.fetches += 1
    
    def increaseCheckTS(self):
        self.last_check_ts += 1
    
    def fail(self):
        self.fails += 1
        if self.fails > max_fails:
            self.status = True
            
    def isFailed(self):
        return self.fails > max_fails
    
    def reset(self):
        self.last_check_ts = self.nr_checks = self.fails = self.fetches = 0
        self.fetch_duration = self.post_duration = self.work_duration = 0.0
        self.status = False
    
    def setAggregation(self,aggr):
        self.aggregation = aggr
    
    def getAggregation(self):
        return self.aggregation
    
    def getTimeseries(self):
        return self.timeseries
    
    def getSession(self):
        return self.session
       
    def getStatus(self):
        return self.status
    
    def updateStatus(self,st):
        self.status = st
        
    def addCheck(self):
        self.nr_checks += 1


# simple queue to load/hold different metrics that should be processed
# loads metric names from a configuration file and tracks the status of every metric/processing
# by default the Metric Queue is displaying it's processing status:
# - metrics that are currently processed/fetched [*]
# - metrics that have failed too often and will not be attempted anymore [F]
# - metrics that are currently not worked on [ ]
# 
# the queue always returns metrics with the oldest last check timestamp to worker threads
# but only if the timestamp is older than the defined chacek_interval to avoid unnecessary/frequent
# fetches from Dynatrace
#
class MetricQueue(Process):
    # ensure there is a data export token on the tenant - create one
    def createExportToken(self, tenantid):
        tenantkey = 'metricapitoken-'+str(tenantid)
        
        # get tenant token from cache 
        tenanttoken = entitycache.get(tenantkey)
        
        if tenanttoken is not None:
            tenantapitoken = tenanttoken.decode('utf-8')
            logger.info('Found API token in cache: {}:{}'.format(tenantid,tenantapitoken))
            return tenantapitoken
        
        tokensession = requests.Session()
        tokensession.trust_env = False
        url = server+'/api/v1.0/control/tenantManagement/tokens/'+tenantid
        headers = {'Authorization':'Api-Token '+apitoken,  'Content-Type':'application/json', 'Accept':'application/json'}
        tokendto = '{ \
            "scopes": ["DataExport"], \
            "label": "metricexport-'+time.strftime('%H%M%S')+'", \
            "expirationDurationInSeconds": "'+str(tokenduration)+'" \
        }'
        
        try:
            response = tokensession.post(url, data=tokendto, headers=headers, timeout=30, verify=False)
            token = response.json()
            tenantapitoken = str(token['tokenId'])
            # put tenant token in cache to avoid too many creations on DT side,
            # make sure cache expiry is lower than DT token expiry time
            tokenexpiry = int(round(tokenduration*0.8))
            logger.info('Putting API token into cache: {}:{} (Expiry: {})'.format(tenantkey,tenantapitoken,tokenexpiry))
            entitycache.setex(tenantkey,tokenexpiry,tenantapitoken)
        except:
            logger.error("Problem creating tenant apitoken {}".format(sys.exc_info()[0]))
        
        tokensession.close()
        return tenantapitoken
        
        
    # get all active tenants
    def getDTTenants(self):
        tenantconfigs = {}
        # get tenants from cache
        ctenants = entitycache.get('tenants')
        if ctenants is not None:
            tenants = json.loads(ctenants)
            return tenants
            
        
        tenantsession = requests.Session()
        tenantsession.trust_env = False
    
        url = server+'/api/v1.0/control/tenantManagement/tenantConfigs'
        headers = {'Authorization':'Api-Token '+apitoken,  'Accept': 'application/json'}
        
        try:
            response = tenantsession.get(url, headers=headers, timeout=30, verify=False)
            tenants = response.json()
            for tenant in tenants:
                if tenant['isActive']:
                    tenantconfigs.update({tenant['tenantUUID']:self.createExportToken(tenant['tenantUUID'])})
            
            logger.info('Tenants: {}'.format(json.dumps(tenantconfigs)))    
            entitycache.setex('tenants',tenantexpiry,json.dumps(tenantconfigs))
        except:
            logger.error("Problem getting tenantconfigs {}".format(sys.exc_info()[0]))
        
        tenantsession.close()
        return tenantconfigs
        
    
    # run the metricqueue
    # load metrics from a file and then continuously iterate over the metrics assigning them to a set of workers
    # these workers can fetch the metrics in parallel so it fixes Pythons GLI limitation of one process/thread
    def run(self):
        self.metrics = {}
        # initialize list of metrics
        try:
            with open("metrics.conf",'r') as fp:
                line = fp.readline()
                while line:
                    line.strip()
                    if line[0] != '#':
                        line.replace(' ','')
                        metr = line.split(',')
                        self.addMetric(metr[0],metr[1])
                    line = fp.readline()
        except:
            logger.error("Unexpected error: {}".format(sys.exc_info()[0]))
            
        # create worker processes
        self.workers = {}
        while True:
            # create a maximum # of workers
            if len(self.workers) < worker_threads:
                # first get all active tenants
                tenants = self.getDTTenants()
            
                metricstate = self.getNext()
                if not (metricstate is None):
                    now = time.time()
                    #w = Process(target=getmetrics, args=(metricstate, backlog,))
                    w = Process(target=getMultiTenantMetric, args=(metricstate, tenants,))
                    self.workers.update({metricstate.getTimeseries() : w})
                    w.__setattr__('starttime',now)
                    w.daemon = True
                    #logger.info('Creating Worker Process for: {} {} Backlog: {}'.format(metricstate.getTimeseries(), metricstate.getAggregation(), backlog))
                    w.start()
            
            # check for finished worker processes
            done = []
            for ts, w in self.workers.items():
                if not w.is_alive():
                    done.append(ts)
                    self.updateMetric(ts, round(time.time()-w.__getattribute__('starttime'),1) ,0.0,0.0)
                    #logger.info('Worker finished for: {} Time: {}'.format(ts, round(time.time()-w.__getattribute__('starttime'),1)))
                    
            # remove the workers that are finished
            for d in done:
                del self.workers[d]
                
            time.sleep(1.0)

    # return the metric that was checked the longest time ago
    # also make sure that it only returns if it's older than the check interval
    # this is not efficient for long list of metrics :-)
    def getNext(self):
        now = oldest = time.time()
        oldest_metric = ''
        for metric,state in self.metrics.items():
            if not state.getStatus():
                if state.getLastCheckTS() < now - check_interval and state.getLastCheckTS() < oldest:
                    oldest = state.getLastCheckTS()
                    oldest_metric = metric
                    
        if oldest_metric:
            self.metrics.get(oldest_metric).updateStatus(True)
            return self.metrics.get(oldest_metric)
        return None
        
    # update the metric state
    # lock/processing state, latest check time
    def updateMetric(self,metric,f_duration,w_duration,p_duration):
        m = self.metrics.get(metric)
        m.addCheck()
        m.updateStatus(False)
        m.updateCheckTS(time.time())
        m.updateFetches()
        m.updateFetchDuration(f_duration)
        m.updateWorkDuration(w_duration)
        m.updatePostDuration(p_duration)
    
        
    def failMetric(self,metric):
        self.metrics.get(metric).addCheck()
        self.metrics.get(metric).updateStatus(False)
        self.metrics.get(metric).fail()
    
    def display(self):
        # output all metrics and their state
        metricstates = ''
        for metric,state in self.metrics.items():
            if state.status:
                s = '*'
            else:
                s = ' '
            if state.isFailed():
                s = 'F'
            metricstates += '[{}] {}\tftime: {}\twtime: {}\tptime: {}\tchecks:{}\tfetches:{}\tfails:{}\t\t{} : {}\n'.format(s, time.ctime(state.last_check_ts) , state.fetch_duration, state.work_duration, state.post_duration, state.nr_checks, state.fetches, state.fails, metric, self.getAggregation(metric))
      
        return metricstates
    
    def addMetric(self,metric,aggr):
        self.metrics.update({ metric: MetricState(metric,aggr) })
    
    def getAggregation(self,metric):
        return self.metrics.get(metric).getAggregation()
    
    def getTimeseries(self,metric):
        return self.metrics.get(metric).getTimeseries()
    
    def getSession(self,metric):
        return self.metrics.get(metric).getSession()


# randomizing the cachec expiry time will help to distribute entity info fetches
def randomizeExpiryDuration(duration):
    expirytime = int(duration + duration*random.uniform(-1*cache_expiry_deviation,cache_expiry_deviation))
    return expirytime

def getHostEntityTags(entity):
    tags = {}
    tags.update({'host':entity[0]['displayName']})
        
    for tag in entity[0]['tags']:
        if 'key' in tag and 'value' in tag:
            tags.update({tag['key'].lower():tag['value']})
            
    #workaround for previous tag names/values, duplicate env to environment
    if 'env' in tags:
        tags.update({'environment':tags['env']})

    return tags

def getHostEntity(session,tenant,token,entityID,reqcount,duration):
    if not "HOST" in entityID:
        return {}
    
    # check if entity is in cache
    citem = entitycache.get(entityID)
    if citem is not None:
        entity = json.loads(citem)
        tags = getHostEntityTags(entity)
        if tags:
            return tags
    
    tags = {}
    url = server+'/e/'+tenant+'/api/v1/entity/infrastructure/hosts?entity='+entityID
    headers = {'Authorization':'Api-Token '+token}
        
    start = time.time()    
    response = session.get(url, headers=headers, timeout=30, verify=False)
    if 200 == response.status_code:
        reqcount[0] +=1
        end = time.time()
        duration[0] += end-start
        
        entity = response.json()
        tags = getHostEntityTags(entity)
        entitycache.setex(entityID, randomizeExpiryDuration(cache_expiry), json.dumps(entity))
    else:
        logger.warning(response)
    
    return tags
    

# fetches information about this entity and returns a key: value hash
def getProcessEntity(session,tenant,token,entityID,reqcount,duration):
    if not "PROCESS_GROUP_INSTANCE" in entityID:
        return {}
    
        # check if entity is in cache
    citem = entitycache.get(entityID)
    if citem is not None:
        tags = json.loads(citem)
        if tags:
            return tags
    
    tags = {}
    url = server+'/e/'+tenant+'/api/v1/entity/infrastructure/processes?entity='+entityID
    headers = {'Authorization':'Api-Token '+token}
    
    start = time.time()    
    response = session.get(url, headers=headers, timeout=30, verify=False)
    if 200 == response.status_code:
        reqcount[0] +=1
        end = time.time()
        duration[0] += end-start
        
        entity = response.json()
        tags.update({'process_group_instance':entity[0]['displayName']})
        if 'isProcessOf' in entity[0]['fromRelationships']:
            tags.update(getHostEntity(session,tenant,token,entity[0]['fromRelationships']['isProcessOf'][0],reqcount,duration))
        
        entitycache.setex(entityID, randomizeExpiryDuration(cache_expiry), json.dumps(tags))
    else:
        logger.warning(response)
    return tags

def getServiceEntity(session,tenant,token,entityID,reqcount,duration):
    if not "SERVICE" in entityID:
        return {}
    
    # check if entity is in cache
    citem = entitycache.get(entityID)
    if citem is not None:
        tags = json.loads(citem)
        if tags:
            return tags
        
    tags = {}
    url = server+'/e/'+tenant+'/api/v1/entity/services?entity='+entityID
    headers = {'Authorization':'Api-Token '+token}
    
    start = time.time()    
    response = session.get(url, headers=headers, timeout=30, verify=False)
    if 200 == response.status_code:
        reqcount[0] +=1
        end = time.time()
        duration[0] += end-start
        
        entity = response.json()
        tags.update({'service':entity[0]['displayName']})
        tags.update({'servicetype':entity[0]['serviceType']})
        
        for tag in entity[0]['tags']:
            if 'key' in tag and 'value' in tag:
                tags.update({tag['key'].lower():tag['value']})
        
        if 'softwareTechnologies' in entity[0]:
            for technology in entity[0]['softwareTechnologies']:
                if 'type' in technology and 'version' in technology:
                    if technology['version'] is None:
                        version = 'n/a'
                    else:
                       version = technology['version'] 
                    tags.update({technology['type'].lower():version})
                    
        entitycache.setex(entityID, randomizeExpiryDuration(cache_expiry), json.dumps(tags))
    else:
        logger.warning(response)
    return tags

def getApplicationEntity(session,tenant,token,entityID,reqcount,duration):
    if not "APPLICATION" in entityID:
        return {}
    
    tags = {}
    return tags
    
def getProcessGroupEntity(session,tenant,token,entityID,reqcount,duration):
    if not "PROCESS_GROUP" in entityID:
        return {}
    
    tags = {}
    return tags


def getEntity(session,tenant,token,entityID,reqcount,duration):
      
    try:
        if "PROCESS_GROUP_INSTANCE" in entityID:
            return getProcessEntity(session,tenant,token,entityID,reqcount,duration)
        if "HOST" in entityID:
            return getHostEntity(session,tenant,token,entityID,reqcount,duration)
        if "SERVICE" in entityID:
            return getServiceEntity(session,tenant,token,entityID,reqcount,duration)
        if "APPLICATION" in entityID:
            return getApplicationEntity(session,tenant,token,entityID,reqcount,duration)
        if "PROCESS_GROUP" in entityID:
            return getProcessGroupEntity(session,tenant,token,entityID,reqcount,duration)
    except:
        logger.error(traceback.format_exc()) 
        
    return {}


def createDatabase(influxhost,session,db):
    
    influxquery = influxhost + ':' + influxdb_read_port + '/query?' + 'u=' + influxuser + '&p=' + influxpass + '&'
    
    create_query = 'q=CREATE+DATABASE+"'+str(db)+'"'
    resp = session.post(influxquery + create_query)
    dbcreate = resp.json()
    result = dbcreate['results']
    if 'error' in result[0]:
        logger.info(result[0]['error'])
        return False
    
    # ensure that the retention policy is set to 90 days
    # note that current InfluxDBs use 'autogen' where older versions use 'default' as RP   
    policy_query = 'q=ALTER+RETENTION+POLICY+autogen+ON+"'+str(db)+'"+DURATION+90d+REPLICATION+1+DEFAULT'
    resp = session.post(influxquery + policy_query)
    dbcreate = resp.json()
    result = dbcreate['results']
    if 'error' in result[0]:
        logger.info(result[0]['error'])
        return False
    
    return True
    
def checkOrCreateDB(influxhost,session,db):
    # this needs improvement to check first if DB esists and only then create the DB
    # also altering and creating retention policies needs to be done in a more efficient way
    state = True
    
    # using cache to reduce influx queries
    dbcachekey = 'influxdb'+str(db)
    dbitem = entitycache.get(dbcachekey)
    if dbitem is not None:
        return True
    
    logger.info('Database ['+str(db)+'] not in cache, so it should exist in influxdb, checking and if needed creating')
    influxquery = influxhost + ':' + influxdb_read_port + '/query?' + 'u=' + influxuser + '&p=' + influxpass + '&'
    
    # if this returns OK then the DB does exist
    check_db_query = 'db='+db+'&q=SELECT+host+FROM+host+LIMIT+1'
    resp = session.get(influxquery + check_db_query)
    dbcheck = resp.json()
    result = dbcheck['results']
    if 'error' in result[0]:
        logger.info(result[0]['error']+', creating now!')
        state = createDatabase(influxhost,session,db)
    
    entitycache.setex(dbcachekey, cache_expiry, True) 
    return state
         

def influxdbpost(session,db,data):
    # ensure the database exists or is created
    
    for influxhost in influxdbs:
        if influxhost == 'dummy':
            continue
        if checkOrCreateDB(influxhost,session,db):
            try:
                #influxwrite = influxhost + '/write?db=' + str(db)
                influxwrite = influxhost + ':' + influxdb_write_port + '/write?db=' + str(db) + '&u=' + influxuser + '&p=' + influxpass
                #logger.info(influxwrite)
                resp = session.post(influxwrite, data=data, timeout=20)
                if 200 <= resp.status_code <= 204:
                    continue
                else: 
                    logger.error('InfluxDB post not OK: {} {}'.format(resp.status_code,influxwrite))
                
            except:
                logger.error('{} \n {}'.format(influxdb+str(db),data))
                logger.error(traceback.format_exc())

def isBlacklisted(entity):
    for pgi in blacklist:
        if pgi in entity:
            return True
        
    return False


# return the timestamp which the query should start with
# eventually adding a bit of extra overlap
def getStartTimestamp(tenant,timeseries):
    ts = getLastCheckTimestamp(tenant, timeseries)
    return ts - backlog_shift
    
def getEndTimestamp(tenant,timeseries):
    return time.time() - backlog_shift
    
    
def getLastCheckTimestamp(tenant,timeseries):
    ts = entitycache.get('LCTS-'+tenant+'-'+timeseries)
    if ts is None:
        ts = time.time() - backlog_window 
    return float(ts)

# store the last check time (in s) for metric/tenant in cache    
def setLastCheckTimestamp(tenant,timeseries):
    entitycache.set('LCTS-'+tenant+'-'+timeseries,time.time())

# iterate through multiple tenants to fetch metrics
def getMultiTenantMetric(mymetric,tenants):
    for tenant,token in tenants.items():
        #logger.info("Getting {} from {} {}".format(mymetric.getTimeseries(), tenant, token))
        getmetrics(mymetric,tenant,token)
    

# Get the metric from Dynatrace, store it in influxdb
def getmetrics(mymetric,tenant,token):
    # randomize some process startup dealy to avoid hitting the Dynatrace API heavily upon startup
    time.sleep(random.randint(1,10))
    
    requestcount = [0]
    postcount = metriccount = 0
    now = time.time()
    valuemap = { 'AVAILABLE':1,'NOT_AVAILABLE':0,'UNMONITORED':-1, 'MONITORING_SIGNAL_LOST':-2, 'None':0 }
    p_duration = 0.0
    f_duration = [0.0]
    w_duration = 0.0
    timeseries = mymetric.getTimeseries()
    aggregation = mymetric.getAggregation()
    #starttimestamp = str(int(round((now-backlog) * 1000)))
    #endtimestamp = str(int(round((now-60) * 1000)))
    
    starttime = getStartTimestamp(tenant, timeseries)
    endtime = getEndTimestamp(tenant, timeseries)
    starttimestamp = int(math.floor(starttime)*1000)    # in ms
    endtimestamp = int(math.ceil(endtime)*1000)         # in ms
    setLastCheckTimestamp(tenant, timeseries)
    backlog = int((endtimestamp-starttimestamp)/1000)      # backlog in s
    if backlog < backlog_window_min:
        backlog = backlog_window_min
        starttimestamp = endtimestamp - backlog_window_min*1000
    
    if backlog > backlog_window:
        backlog = backlog_window
        starttimestamp = endtimestamp - backlog_window*1000
            
    session = mymetric.getSession()
    
    jsondata = '{ \
        "timeseriesId":"'+timeseries+'",\
        "aggregationType":"'+aggregation+'",\
        "startTimestamp":"'+str(starttimestamp)+'",\
        "endTimestamp":"'+str(endtimestamp)+'"\
        }'
    url = server+'/e/'+tenant+'/api/v1/timeseries/'
    headers = {'Authorization':'Api-Token '+token, 'Content-type': 'application/json'}
    parameters = {'includeData':'true','aggregationType':aggregation.lower(),'startTimestamp':str(starttimestamp), 'endTimestamp':str(endtimestamp)}

    metricset = {}
    entityset = {}

    line = ''
    cnt = 0

    if "builtin" in timeseries:
        measurement = timeseries.split(':')[1].rsplit('.',1)[0].lower()
    else:
        measurement = str(timeseries.split('.',1)[1]).lower().replace(':','.').rsplit('.',1)[0]    
        
    try:    
        #session = requests.Session()
        #session.trust_env = False # disable any environments set proxy
        start = time.time()
        #response = session.post(url, data=jsondata, headers=headers, timeout=60, verify=False)
        response = session.get(url+timeseries,headers=headers, params=parameters, timeout=60, verify=False)
        
        requestcount[0] += 1
        end = time.time()
        f_duration[0] = end - start

        if 200 <= response.status_code <= 204:
            metrics = response.json()
            # first get additional information on entities
            for entity in metrics['dataResult']['entities']:
                tags = {}
                # additional calls required for: host, process_group_instance, service, application, process_group
                # this needs some filtering to NOT request every PGI (e.g.ignore OneAgent etc...)
                if not isBlacklisted(metrics['dataResult']['entities'][entity.strip()]):
                    tags.update(getEntity(session,tenant, token, entity,requestcount,f_duration))
                
                tagname = entity.split('-')[0].lower()
                
                # tags is empty if entity is one of disk, network_interface
                if not tagname in tags:
                    tagvalue = metrics['dataResult']['entities'][entity.strip()].replace(' ', '\ ')
                    tags.update({tagname:tagvalue})
                    
                entityset.update({entity:tags})
                
            start = time.time()
            for datapoint in metrics['dataResult']['dataPoints']:
                # get values/names for entities
                tags = {}
                for entity in datapoint.split(','):
                    entity = entity.strip()
                    # check if entity is a key=value pair
                    if '=' in entity:
                        # removing quotes from tags
                        entity = entity.replace("'", "")
                        tags.update({entity.split("=")[0]:entity.split("=")[1]})
                    else:
                        # merge potential tagsets of multiple entities
                        tags.update(entityset[entity])
    
                for metric in metrics['dataResult']['dataPoints'][datapoint]:
                    # for cases where Dynatrace returns strings e.g. AVAILABLE
                    if str(metric[1])[0].isalpha():
                        value = valuemap.get(str(metric[1]),0)
                    else:
                        value = metric[1]
                        
                    if 'customer' in tags:
                        if not tags['customer'] in metricset:
                            metricset.update({tags['customer']:set()})
                    
                        line = metricset[tags['customer']]
                        # ToDo: there are more characters that need escaping in the inlfux line protocol for now only (' ' and ',') ae escaped
                        tagstr = ','.join('{}={}'.format(key, value.replace(' ','\ ').replace(',','\,')) for key,value in tags.items())
                        line.update({measurement + ',' + tagstr + ' ' + timeseries.split(':')[1] + '=' + str(value) + ' ' + str(metric[0] * 1000000)})
                        metricset.update({tags['customer']:line})
                    #else:
                        #logger.info(tags)
    		
                    cnt += 1
            
            session.close()
            postsession = requests.Session()
            postsession.trust_env = False # disable any environments set proxy
            
            for db,line in metricset.items():
                #build dataline from lineset
                linesize = 0
                dataline = ''
                for l in line:
                    dataline += l + '\n'
                    linesize += 1
                    if linesize > 200:
                        pstart = time.time()
                        influxdbpost(postsession,db,dataline)
                        # always post a copy to the BI database "HCS"
                        influxdbpost(postsession,'hcs',dataline)
                        pend = time.time()
                        p_duration += pend-pstart
                        postcount += 1
                        dataline = ''
                        linesize = 0
                        
                if dataline:
                    pstart = time.time()
                    influxdbpost(postsession,db,dataline)
                    # always post a copy to the BI database "HCS"
                    influxdbpost(postsession,'hcs',dataline)
                    pend = time.time()
                    p_duration += pend-pstart
                    postcount += 1
                    
            postsession.close()
                    
            end = time.time()
            f_duration[0] = round(f_duration[0],1)
            w_duration = round(end-start-p_duration,1)
            p_duration = round(p_duration,1)              # post duration
            
        else:
            logger.warning('Timeseries {} query returned HTTP status: {}'.format(timeseries,response.status_code))
            
    except:
        logger.error(traceback.format_exc()) 

    metriccount += cnt       
    logger.info("(f: {} w: {} p: {}) {} Timeseries: {} Timeframe [{} - {} ({}s)] Requests: {} Posts: {} Metrics: {}".format(f_duration[0],w_duration,p_duration,tenant,timeseries.split(':')[1],time.strftime('%H:%M:%S',time.gmtime(starttimestamp/1000)),time.strftime('%H:%M:%S',time.gmtime(endtimestamp/1000)),backlog,requestcount[0],postcount,metriccount))

def main(argv):
    # for simultaneous post to multiple InfluxDBs
    # if env variable INFLUXDB_URL is a list
    global influxdbs
    influxdbs = [db.strip() for db in influxdb.split(',')]
    
    logger.info('Configuration: {} {} --> {}'.format(server, apitoken, influxdbs))
    logger.info('Worker Processes: {} Check-Interval: {}s Backlog: {}s Token-Duration: {}s'.format(worker_threads, check_interval, backlog_window,tokenduration))
    
    metricqueue = MetricQueue()
    metricqueue.start()
    #print(metricqueue.display())
    
    metricqueue.join()
    
if __name__ == "__main__":
   main(sys.argv[1:])
