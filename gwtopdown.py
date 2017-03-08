import getpass, ConfigParser, sys, requests, json, time, logging, datetime, os
from requests import exceptions
requests.packages.urllib3.disable_warnings()

# gw class for all the GroupWise REST API functions
class gw:

    # init the class and set some vars and setup the requests REST session
    def __init__(self,gwhost,gwport,gwadmin,gwpass):
        self.gwAdmin = gwadmin
        self.gwPass = gwpass
        self.baseUrl = 'https://%s:%s' % (gwhost,gwport)
        self.s = requests.Session()
        self.s.auth = (gwadmin, gwpass)
        self.s.verify = False
        self.s.headers = {'Content-Type': 'application/json',
              'Accept': 'application/json'}

    # function to write to log file and print to console
    def logit(self,msg):
        print msg
        logging.info(msg)

    # function to parse json response with a list of objects
    def listparse(self,response):
        if response.text:
            j = json.loads(response.text)
            if 'error' in j.keys():
                return j['statusMsg']
            else:
                if 'object' in j.keys():
                    return j['object']
                else:
                    return 1

    # function to parse json response with only one object
    def parseone(self,response):
        if response.text:
            j = json.loads(response.text)
            if 'error' in j.keys():
                return j['statusMsg']
            else:
                return j

    #  check administrator level to ensure it's a GW system admin
    def whoami(self):
        url = '%s/gwadmin-service/system/whoami' % self.baseUrl
        r = self.s.get(url)
        if r.text:
            j = json.loads(r.text)
            if 'error' in j.keys():
                self.logit("Error with admin authentication:  %s" % j['statusMsg'])
                sys.exit()
            else:
                userRoles = self.parseone(r)
                if 'SYSTEM_RECORD' not in userRoles['roles']:
                    self.logit("%s is not a GroupWise system administrator" % self.gwAdmin)
                    self.logit("You must enter a system administrator to Run this Utility.")
                    self.logit("Please enter valid system administrator credentials.")
                else:
                    self.logit("Authenticated to %s as %s\n" % (self.baseUrl, self.gwAdmin))

    # get status of a GW agent
    def agentCheck(self, url):
        r = self.s.get(url)
        status = self.parseone(r)
        if 'serviceState' in status.keys():
            return status['serviceState']
        else:
            return 1

    # start a GW agent
    def stopAgent(self, url):
        counter = 0
        stopUrl = '%s?command=STOP' % url
        r = self.s.get(stopUrl)
        while counter <= 30:
            state = self.agentCheck(url)
            if state == 'STOPPED':
                return state
            else:
                time.sleep(1)
                counter += 1
        if state != 'STOPPED':
            return state

    # stop a GW agent
    def startAgent(self, url):
        counter = 0
        startUrl = '%s?command=START' % url
        r = self.s.get(startUrl)
        while counter <= 30:
            state = self.agentCheck(url)
            if state == 'STARTED':
                return state
            else:
                time.sleep(2)
                counter += 1
        if state != 'STARTED':
            return state

    # find any GWIA's for a domain
    def getGwia(self,dom):
        gwiaUrl = '%s/gwadmin-service/domains/%s/gwias' % (self.baseUrl, dom)
        r = self.s.get(gwiaUrl)
        objects = self.listparse(r)
        if objects != 1:
            gwialist = {}
            for object in objects:
                gwialist[object['name']] = object['@url']
            return gwialist
        else:
            return 1

    # rebuild the domain or po db.
    def rebuild(self, adminUrl, dom, *pos):
        def status(location):
            results = self.s.get(location)
            status = json.loads(results.text)['done']
            return status

        if not pos:
            url = '%s/gwadmin-service/domains/%s/maintenance' % (self.baseUrl, dom)
        else:
            for po in pos:
                url = '%s/gwadmin-service/domains/%s/postoffices/%s/maintenance' % (adminUrl, dom, po)
        data = {'action':'REBUILD'}
        r = self.s.post(url, data=json.dumps(data))
        loc = r.headers['location']
        rebuildState = False
        while rebuildState == False:
            rebuildState = status(loc)
            time.sleep(1)
        return rebuildState

    # rebuild all the po's in a domain
    def rebuildPos(self, adminUrl, domName):
        getPos = '%s/gwadmin-service/domains/%s/postoffices' % (adminUrl, domName)
        r = self.s.get(getPos)
        if r.text:
            j = json.loads(r.text)
            if j['resultInfo']['outOf'] != 0:
                poObjects = j['object']
            else:
                self.logit("No post offices found belonging to %s\n" % domName)
                poObjects = 1
        else:
            self.logit("Unable to get list of post offices for %s" % domName)
            self.logit("Skipping rebuild of post offices for domain: %s \n" % domName)
            return 0
        if poObjects != 1:
            self.logit("Rebuilding post offices for %s Domain\n" % domName)
            for po in poObjects:
                rbstatus = 0
                poName = po['name']
                poUrl = po['@url']
                getPoas = '%s/%s/poas' % (getPos, poName)
                r = self.s.get(getPoas)
                poas = self.listparse(r)
                if poas != 1:
                    for poa in poas:
                        if rbstatus != 1:
                            poaAdminUrl = 'https://%s:%s/gwadmin-service/system' % (poa['ipAddress'], poa['adminPort'])
                            try:
                                poAdminCheck = self.s.get(poaAdminUrl)
                            except requests.ConnectionError, e:
                                self.logit('Connection to PO admin service Failed.')
                                self.logit('Unable to shutdown %s %s.  Skipping rebuild for %s' % (poName, poa['name'], poName))
                                rbstatus = 1
                            if rbstatus == 0:
                                agentUrl = '%s/%s/manage' % (getPoas, poa['name'])
                                state = self.agentCheck(agentUrl)
                                self.logit('Starting rebuild of %s' % poName)
                                state = self.agentCheck(agentUrl)
                                #self.logit('POA is in state: %s' % state)
                                if state != 'STOPPED':
                                    self.logit( 'Stopping %s %s' % (poName, poa['name']))
                                    state = self.stopAgent(agentUrl)
                                    if state == 'STOPPED':
                                        self.logit('%s %s Stopped' % (poName, poa['name']))
                                        rbstatus = 0
                                    else:
                                        self.logit('Could not stop %s %s within 30 seconds.' % (poName, poa['name']))
                                        self.logit('Skipping this post office rebuild')
                                        rbstatus = 1
                if rbstatus == 0:
                    self.logit('Rebuilding %s wphost.db' % poName)
                    status = self.rebuild(adminUrl,domName, poName)
                    if status == True:
                        self.logit('Completed rebuild of %s' % poName)
                    else:
                        self.logit('Rebuild of %s Failed' % poName)
                    #self.logit('Restarting agents for %s' % poName)
                    for poa in poas:
                        agentUrl = '%s/%s/manage' % (getPoas, poa['name'])
                        self.logit('Starting %s %s' % (poName, poa['name']))
                        state = self.startAgent(agentUrl)
                        if state == 'STARTED':
                            self.logit('%s %s Started \n' % (poName, poa['name']))
                        else:
                            self.logit('%s %s not started after 30 seconds' % (poName, poa['name']))
                            self.logit('You may want to manually check the agent status\n')
            self.logit('Completed post office rebuilds for %s domain \n' % domName)
        return 0

    # find the primary domain and rebuild any po's it has
    def getPri(self):
        url = '%s/gwadmin-service/list/domain?domainType=PRIMARY' % self.baseUrl
        try:
            r = self.s.get(url)
        except requests.ConnectionError, e:
            self.logit("Connection Error: %s" % e)
            sys.exit()
        objects = self.listparse(r)
        if objects != 1:
            self.priDom = objects[0]['name']

            self.priAdminUrl = self.getHost(self.priDom)

            self.logit('Connected to primary domain admin service using URL: %s \n' % self.priAdminUrl)
        else:
            self.logit('Failed to get primary domain information.  Exiting.')
            sys.exit()

        self.rebuildPos(self.priAdminUrl, self.priDom)

    # get the  ip and admin port for a domain
    def getHost(self,dom):
        url = '%s/gwadmin-service/domains/%s/mta' % (self.baseUrl, dom)
        r = self.s.get(url)
        mta = self.parseone(r)
        if mta != 1:
            domHost = 'https://%s:%s' % (mta['ipAddress'], mta['adminPort'])
            return domHost

    # find any secondary domains and omit from the rebuild list if the domain version is less than 1400
    def getSecondarys(self):
        priUrl = '%s/gwadmin-service/list/domain?domainType=SECONDARY' % self.baseUrl
        try:
            d = self.s.get(priUrl)
        except requests.ConnectionError, e:
            self.logit("Error connecting to URL: %s" % priUrl)
            self.logit(e)
        if d.text:
            j = json.loads(d.text)
            if 'error' in j.keys():
                self.logit('Error getting secondary domains: %s' % j['statusMsg'])
                return 1
            if j['resultInfo']['outOf'] == 0:
                self.logit('No secondary domains found')
                return 1
        else:
            self.logit('Could not get list of secondary domains')

        secondarys = self.listparse(d)
        if secondarys != 1:
            doms = []
            for secondary in secondarys:
                if int(secondary['domainVersion']) < 1400:
                    self.logit("Domain verision is not 1400 or greater.")
                    self.logit('Skipping rebuild of %s and post offices' % secondary['name'])
                else:
                    doms.append(secondary['name'])
            return doms
        else:
            return 1

    # rebuild the secondary domains and it's post offices
    def rebuildDomains(self):
        domlist = self.getSecondarys()
        if domlist == 1:
            return 1
        domains = {}
        for domain in domlist:
            m = self.getHost(domain)
            domains[domain] = m
        for domName, adminUrl in domains.items():

            self.logit("Starting rebuild of %s domain" % domName)

            try:
                adminStatus = self.s.get(adminUrl + '/gwadmin-service/system/whoami')
            except exceptions.ConnectionError, e:
                self.logit("Connection to %s admin service failed" % domName)
                self.logit(e)
                self.logit("Skipping rebuild for %s\n" % domName)
                break
            #self.logit('Starting rebuild of %s domain' % domName)
            agentUrl = '%s/gwadmin-service/domains/%s/mta/manage' % (self.baseUrl, domName)
            #self.logit('Checking status of %s MTA' % domName)
            state = self.agentCheck(agentUrl)
            #self.logit('%s MTA is in state: %s' % (domName, state))
            if state != 'STOPPED':
                self.logit('Stopping %s MTA' % domName)
                state = self.stopAgent(agentUrl)
                if state == 'STOPPED':
                    self.logit('%s MTA Stopped' % domName)
                else:
                    self.logit('Could not stop %s MTA within 60 seconds.' % domName)
                    self.logit('Skipping rebuild for %s\n' % domName)
                    break
            #self.logit("Checking for any GWIA's for %s" % domName)
            gwias = self.getGwia(domName)
            if gwias != 1:
                for gwiaName, gwiaUrl in gwias.items():
                    #self.logit('Checking status of %s.%s' % (domName, gwiaName))
                    gwiaManageUrl = '%s%s/manage' % (self.baseUrl, gwiaUrl)
                    state = self.agentCheck(gwiaManageUrl)
                    #self.logit('%s.%s is in state: %s' % (domName, gwiaName, state))
                    if state != 'STOPPED':
                        self.logit('Stopping %s %s' % (domName, gwiaName))
                        state = self.stopAgent(gwiaManageUrl)
                        if state == 'STOPPED':
                            self.logit('%s.%s stopped' % (domName, gwiaName))
                        else:
                            self.logit('Could not stop %s %s within 30 seconds' % (domName, gwiaName))
                            self.logit('Skipping rebuild for %s\n' % domName)
                            self.logit('Starting %s MTA' % domName)
                            state = self.startAgent(agentUrl)
                            if state == 'STARTED':
                                self.logit('%s MTA Started' % domName)
                            else:
                                self.logit('%s MTA not started after 30 seconds' % domName)
                                self.logit('You may want to manually check the agent status')
                            break
            self.logit('Rebuilding %s wpdomain.db' % domName)
            status = self.rebuild(self.baseUrl,domName)
            if status == True:
                self.logit('Rebuild of %s complete' % domName)
            else:
                self.logit('Rebuild of %s Failed' % domName)
                #break
            #self.logit('Restarting agents for %s' % domName)
            self.logit('Starting %s MTA' % domName)
            state = self.startAgent(agentUrl)
            if state == 'STARTED':
                self.logit('%s MTA Started' % domName)
            else:
                self.logit('%s MTA not started after 30 seconds' % domName)
                self.logit('You may want to manually check the agent status')
            if gwias != 1:
                for gwiaName, gwiaUrl in gwias.items():
                    gwiaManageUrl = '%s%s/manage' % (self.baseUrl, gwiaUrl)
                    self.logit('Starting %s %s' % (domName, gwiaName))
                    state = self.startAgent(gwiaManageUrl)
                    if state == 'STARTED':
                        self.logit('%s.%s started' % (domName, gwiaName))
                    else:
                        self.logit('%s %s  not started after 30 seconds' % (domName,gwiaName))
                        self.logit('You may want to manually check the agent status')
            self.logit("Finished with rebuild of %s\n" % domName)

            self.rebuildPos(adminUrl,domName)

if __name__ == "__main__":

    # check if config file is provided on command line - else prompt for it and see if it exists
    if len(sys.argv) == 2:
        configFile = sys.argv[1]
    else:
        configFile = raw_input("Enter config file: ")
    if not os.path.isfile(configFile):
        print "Config file not found.  Please try again."
        sys.exit()

    # get data from config file
    config = ConfigParser.ConfigParser()
    try:
        config.read(configFile)
        host = config.get("gw",'host')
        port = config.get("gw","port")
        gwAdmin = config.get("gw","admin")
    except:
        print "Error reading config file."
        sys.exit()

    # get admin pwd
    gwPass = getpass.getpass("Password for %s: " % gwAdmin)

    # set up log file
    today = datetime.datetime.today()
    d = today.strftime('%m-%d-%Y')
    logging.basicConfig(level=logging.INFO,
                  format='%(asctime)s %(message)s',
                  datefmt='%m/%d/%y %H:%M:%S',
                  filename='%s_gwtopdown.log' % d,
                  filemode='w')

    requestsLog = logging.getLogger('requests.packages.urllib3')
    requestsLog.propagate = False

    # init gw class and fire away.
    gw = gw(host,port,gwAdmin,gwPass)
    gw.whoami()
    gw.getPri()
    gw.rebuildDomains()
    print 'Completed topdown rebuild'
    logging.info('Completed topdown rebuild')


