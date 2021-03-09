import os, types, sys, traceback, getopt, datetime
import NameServerPy
import ConfigMgrPy

NameServerPy.init()

class ServiceStatus:
    NoHSR        = 10
    Error        = 11
    Unknown      = 12
    Initializing = 13
    Syncing      = 14
    Active       = 15
    __strmap={NoHSR:'System Replication not active', Error:'ERROR', Unknown:'UNKNOWN', Initializing:'INITIALIZING', Syncing:'SYNCING', Active:'ACTIVE'}

    ActiveYes = "YES"
    ActiveNo = "NO"

    @staticmethod
    def toStr(i):
        return ServiceStatus.__strmap[i]

    @staticmethod
    def fromStr(s, activeStatus, default = Error): 
        for n, v in ServiceStatus.__strmap.iteritems():
            if s == v:
                return n

        return default

class SystemReplicationStatusUtils(object):
    @staticmethod
    def createTNSClient():
        ns = NameServerPy.TNSClient()
        ns.disableNSLibraryLoad()
        return ns

    @staticmethod
    def determineAndPrintOverallStatus(status, sapcontrol):
        if not sapcontrol:
            print

        overall = ServiceStatus.Active

        # Error
        if isinstance(status, int):
            if sapcontrol:
                print 'status=%s' % ServiceStatus.toStr(status)
            else:
                print 'overall system replication status:', ServiceStatus.toStr(status)
            return status

        if len(status) == 0:
            # --localhost on a primary standby host
            ns = SystemReplicationStatusUtils.createTNSClient()
            mode = ns.getDRMode().upper()
            if mode == "PRIMARY":
                if sapcontrol:
                    print 'overall_replication_status=%s' % ServiceStatus.toStr(ServiceStatus.Active)
                else:
                    print 'overall system replication status:', ServiceStatus.toStr(ServiceStatus.Active)
                return ServiceStatus.Active

            # No replication status entries where found and this site is not a primary
            if sapcontrol:
                print 'status=%s' % ServiceStatus.toStr(ServiceStatus.NoHSR)
            else:
                print 'overall system replication status:', ServiceStatus.toStr(ServiceStatus.NoHSR)
            return ServiceStatus.NoHSR

        for id, st in status.items():
            if sapcontrol:
                print 'site/%d/SITE_NAME=%s' % (id, st["SECONDARY_SITE_NAME"])
                print 'site/%d/SOURCE_SITE_ID=%s' % (id, st["SOURCE_SITE_ID"])
                print 'site/%d/REPLICATION_MODE=%s' % (id, st["REPLICATION_MODE"])
                print 'site/%d/REPLICATION_STATUS=%s' % (id, ServiceStatus.toStr(st["REPLICATION_STATUS"]))
            else:
                print 'status system replication site "%s":' % id, ServiceStatus.toStr(st["REPLICATION_STATUS"])
            if st["REPLICATION_STATUS"] < overall:
                overall = st["REPLICATION_STATUS"]
        if sapcontrol:
            print 'overall_replication_status=%s' % ServiceStatus.toStr(overall)
        else:
            print 'overall system replication status:', ServiceStatus.toStr(overall)
        return overall

    @staticmethod
    def printLocalHSRInformation(sapcontrol):
        ns = SystemReplicationStatusUtils.createTNSClient()
        mode = ns.getDRMode().upper()
        siteId = ns.getDRDatacenter()
        sourceSiteId = ns.drGetSourceSystem()
        primaryMasters = ConfigMgrPy.LayeredConfiguration('global.ini', ConfigMgrPy.CUSTOMER).getStringValue("system_replication_site_masters", str(sourceSiteId))
        primaryMasterWithoutPort = " ".join(x.split(":")[0] for x in primaryMasters.split(" "))
        if sapcontrol:
            print "site/%d/REPLICATION_MODE=%s" % (siteId, mode)
            print "site/%d/SITE_NAME=%s" % (siteId, ConfigMgrPy.LayeredConfiguration('global.ini', ConfigMgrPy.CUSTOMER).getStringValue("system_replication", "site_name"))
            if not mode == "PRIMARY":
                print "site/%d/SOURCE_SITE_ID=%s" % (siteId, sourceSiteId)
                print "site/%d/PRIMARY_MASTERS=%s" % (siteId, primaryMasterWithoutPort)
            print "local_site_id=%d" % siteId
        else:
            print
            print "Local System Replication State"
            print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
            print
            print "mode: %s" % mode
            print "site id: %s" % siteId
            print "site name: %s" % ConfigMgrPy.LayeredConfiguration('global.ini', ConfigMgrPy.CUSTOMER).getStringValue("system_replication", "site_name")
            if not mode == "PRIMARY":
                print "active primary site: %s" % sourceSiteId
                print "primary masters: %s" % primaryMasterWithoutPort

class SystemReplicationStatus(object):
    def getStatusAndPrint(self, longFormat, site, sapcontrol, requestSecondaryActiveStatus = True, local = False):
        # No HSR

        mode = self.getDRMode()
        if not mode:
            if sapcontrol:
                print "SAPCONTROL-OK: <begin>"
                print "local_site_id=0"
            else:
                print "this system is not a system replication site"

            if sapcontrol:
                print "SAPCONTROL-OK: <end>"

            return ServiceStatus.NoHSR

        ns = SystemReplicationStatusUtils.createTNSClient()
        if not self.isNsActive(ns) or not self.isPrimarySystem():
            if sapcontrol:
                print "SAPCONTROL-OK: <begin>"
            else:
                print "this system is either not running or not primary system replication site"

            SystemReplicationStatusUtils.printLocalHSRInformation(sapcontrol)

            if sapcontrol:
                print "SAPCONTROL-OK: <end>"

            return ServiceStatus.Unknown

        if self.hasSecondaries() == 0:
            if sapcontrol:
                print "SAPCONTROL-OK: <begin>"
            else:
                print "there are no secondary sites attached"

            SystemReplicationStatusUtils.printLocalHSRInformation(sapcontrol)

            if sapcontrol:
                print "SAPCONTROL-OK: <end>"

            return ServiceStatus.NoHSR

        config, status = self.getLandscapeConfigurationUpdatedVersion(site, requestSecondaryActiveStatus, local)

        isMultiDb = ConfigMgrPy.LayeredConfiguration('global.ini', ConfigMgrPy.READONLY).getStringValue("multidb", "mode") == "multidb"

        format = []
        names = []
        if isMultiDb:
            format.extend(["DATABASE"])
            names.extend(["Database"])

        if longFormat:
            format.extend(["HOST", "PORT", "SERVICE_NAME", "VOLUME_ID", "SITE_ID", "SITE_NAME", "SECONDARY_HOST", "SECONDARY_PORT", "SECONDARY_SITE_ID", "SECONDARY_SITE_NAME", "SECONDARY_ACTIVE_STATUS", "SECONDARY_CONNECT_TIME", "SECONDARY_RECONNECT_COUNT", "SECONDARY_FAILOVER_COUNT", "REPLICATION_MODE", "REPLICATION_STATUS", "REPLICATION_STATUS_DETAILS", "LAST_LOG_POSITION", "LAST_LOG_POSITION_TIME", "LAST_SAVEPOINT_VERSION", "LAST_SAVEPOINT_LOG_POSITION", "LAST_SAVEPOINT_START_TIME", "SHIPPED_LOG_POSITION", "SHIPPED_LOG_POSITION_TIME", "SHIPPED_LOG_BUFFERS_COUNT", "SHIPPED_LOG_BUFFERS_SIZE", "SHIPPED_LOG_BUFFERS_DURATION", "SHIPPED_SAVEPOINT_VERSION", "SHIPPED_SAVEPOINT_LOG_POSITION", "SHIPPED_SAVEPOINT_START_TIME", "SHIPPED_FULL_REPLICA_COUNT", "SHIPPED_FULL_REPLICA_SIZE", "SHIPPED_FULL_REPLICA_DURATION", "SHIPPED_LAST_FULL_REPLICA_SIZE", "SHIPPED_LAST_FULL_REPLICA_START_TIME", "SHIPPED_LAST_FULL_REPLICA_END_TIME", "SHIPPED_DELTA_REPLICA_COUNT", "SHIPPED_DELTA_REPLICA_SIZE", "SHIPPED_DELTA_REPLICA_DURATION", "SHIPPED_LAST_DELTA_REPLICA_SIZE", "SHIPPED_LAST_DELTA_REPLICA_START_TIME", "SHIPPED_LAST_DELTA_REPLICA_END_TIME", "RESET_COUNT", "LAST_RESET_TIME", "CREATION_TIME"])
            names = format
        else:
            format.extend(["HOST", "PORT", "SERVICE_NAME", "VOLUME_ID", "SITE_ID", "SITE_NAME", "SECONDARY_HOST",  "SECONDARY_PORT",  "SECONDARY_SITE_ID",  "SECONDARY_SITE_NAME",  "SECONDARY_ACTIVE_STATUS",  "REPLICATION_MODE",  "REPLICATION_STATUS",  "REPLICATION_STATUS_DETAILS"])
            names.extend(["Host", "Port", "Service Name", "Volume ID", "Site ID", "Site Name", "Secondary\nHost", "Secondary\nPort", "Secondary\nSite ID", "Secondary\nSite Name", "Secondary\nActive Status", "Replication\nMode", "Replication\nStatus", "Replication\nStatus Details"])

        if sapcontrol:
            print "SAPCONTROL-OK: <begin>"
            for l in config:
                for k, v in l.items():
                    print "service/" + l["HOST"] + "/" + str(l["PORT"]) + "/" + k + "=" + str(v)
        else:
            self.printDictList(config, format, names)

        rc = SystemReplicationStatusUtils.determineAndPrintOverallStatus(status, sapcontrol)
        SystemReplicationStatusUtils.printLocalHSRInformation(sapcontrol)

        if sapcontrol:
            print "SAPCONTROL-OK: <end>"

        return rc

    def isNsActive(self, ns):
        ns.setNoRetries()
        nsActive = False

        try:
            ns.storeTrees([]) # force call to nameserver, else ns.getLandscapeConfiguration() could guess config from obsolete shared memory
            nsActive = True
        except:
            try:
                ns.useMasterNameServer(True) # directly connect to master if this script is executed on a stopped slave host
                ns.storeTrees([])
                nsActive = True
            except:
                pass

        return nsActive

    def getDRMode(self):
        try:
            ns = SystemReplicationStatusUtils.createTNSClient()
            if self.isNsActive(ns):
                return ns.getDRMode().upper()
            else:
                return ""
        except:
            return ""

    def isPrimarySystem(self):
        try:
            ns = SystemReplicationStatusUtils.createTNSClient()
            if self.isNsActive(ns):
                return ns.getSystemReplicationInfo()["mode"].lower() == "primary"
        except:
            return False

    def hasSecondaries(self):
        try:
            ns = SystemReplicationStatusUtils.createTNSClient()
            if self.isNsActive(ns):
                return ns.getSystemReplicationInfo()["numConsumers"]
        except:
            return 0

    def getSystemReplicationStatus(self, requestSecondaryActiveStatus = True, local = False):
        ns = SystemReplicationStatusUtils.createTNSClient()
        host = ""
        if local:
            host = ns.getServiceHost()
        return ns.getSystemReplicationStatus(requestSecondaryActiveStatus, host)

    # Do not touch the signature and return format of this method. It is used by external scripts such as cluster manager
    # Please use: getLandscapeConfigurationUpdatedVersion
    def getLandscapeConfiguration(self, site):
        status = {}
        config = []

        try:
            ns = SystemReplicationStatusUtils.createTNSClient()
            if self.isNsActive(ns):
                config = self.getSystemReplicationStatus()
                config = [row for row in config if not row['VOLUME_ID'] == 0] # do not show standy services as they do not replicate anything
                config.sort(lambda a, b:cmp(a['SITE_ID'], b['SITE_ID']))

                if site != None:
                    config2 = []
                    for row in config:
                        if row["SECONDARY_HOST"].lower() in ["not mapped", "not_mapped"]:
                            continue
                        if row["SECONDARY_SITE_NAME"].lower() == site.lower():
                            config2.append(row)
                    config = config2

                for row in config:
                    if row['REPLICATION_STATUS'] == "STOPPED" or row['REPLICATION_STATUS'] == "TENANTCOPY":
                        continue
                    if row["SECONDARY_HOST"].lower() in ["not mapped", "not_mapped"]:
                        status['NOT MAPPED']= ServiceStatus.Error
                        continue
                    if not status.has_key(row["SECONDARY_SITE_NAME"]):
                        status[row["SECONDARY_SITE_NAME"]] = ServiceStatus.Active
                    service_status = ServiceStatus.fromStr(row['REPLICATION_STATUS'], row['SECONDARY_ACTIVE_STATUS'])
                    if service_status < status[row["SECONDARY_SITE_NAME"]]:
                        status[row["SECONDARY_SITE_NAME"]] = service_status

                    for k, v in row.items():
                        if k.endswith("_TIME"):
                            if v > 0:
                                row[k] = datetime.datetime.fromtimestamp(int(v)/1000.0/1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')
                            else:
                                row[k] = "-"
            else:
                config = []
                status = ServiceStatus.Error

        except Exception, exc:
            traceback.print_exc()
            config = []
            status = ServiceStatus.Error

        return (config, status)

    # This version is internaly used  by systemReplicationStatus.py script
    def getLandscapeConfigurationUpdatedVersion(self, site, requestSecondaryActiveStatus = True, local = False):
        status = {}
        config = []

        try:
            ns = SystemReplicationStatusUtils.createTNSClient()
            if self.isNsActive(ns):
                config = self.getSystemReplicationStatus(requestSecondaryActiveStatus, local)
                config = [row for row in config if not row['VOLUME_ID'] == 0] # do not show standy services as they do not replicate anything
                config.sort(lambda a, b:cmp(a['SITE_ID'], b['SITE_ID']))

                if site != None:
                    config2 = []
                    for row in config:
                        if row["SECONDARY_HOST"].lower() in ["not mapped", "not_mapped"]:
                            continue
                        if row["SECONDARY_SITE_NAME"].lower() == site.lower():
                            config2.append(row)
                    config = config2

                for row in config:
                    if row['REPLICATION_STATUS'] == "STOPPED" or row['REPLICATION_STATUS'] == "TENANTCOPY":
                        continue
                    if row["SECONDARY_HOST"].lower() in ["not mapped", "not_mapped"]:
                        status['NOT MAPPED']= {"SECONDARY_SITE_NAME" : "ERROR",
                         "REPLICATION_MODE" : "ERROR",
                         "REPLICATION_STATUS" : ServiceStatus.Error}
                        continue
                    if not status.has_key(row["SECONDARY_SITE_ID"]):
                        status[row["SECONDARY_SITE_ID"]] = {"SECONDARY_SITE_NAME" : row["SECONDARY_SITE_NAME"],
                             "REPLICATION_MODE" : row["REPLICATION_MODE"],
                             "SOURCE_SITE_ID" : row["SITE_ID"],
                             "REPLICATION_STATUS" : ServiceStatus.Active}
                    service_status = ServiceStatus.fromStr(row['REPLICATION_STATUS'], row['SECONDARY_ACTIVE_STATUS'])
                    if service_status < status[row["SECONDARY_SITE_ID"]]["REPLICATION_STATUS"]:
                        status[row["SECONDARY_SITE_ID"]]["REPLICATION_STATUS"] = service_status

                    for k, v in row.items():
                        if k.endswith("_TIME"):
                            if v > 0:
                                row[k] = datetime.datetime.fromtimestamp(int(v)/1000.0/1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')
                            else:
                                row[k] = "-"
            else:
                config = []
                status = ServiceStatus.Error

        except Exception, exc:
            traceback.print_exc()
            config = []
            status = ServiceStatus.Error

        return (config, status)

    def printDictList(self, table, columns, headers):
        l={} # max. column length
        hr=1 # number of header rows
        for c,h in zip(columns,headers):
            hh=h.split('\n')
            if len(hh)>hr: hr=len(hh)
            for hhh in hh:
                if c not in l:      l[c]=len(hhh)
                elif len(hhh)>l[c]: l[c]=len(hhh)
        for row in table:
            for c in columns:
                if c not in row:                        l_c=1
                elif type(row[c]) in types.StringTypes: l_c=len(    row[c] )
                else:                                   l_c=len(str(row[c]))
                if l_c>l[c]: l[c]=l_c
        print '|',
        for i in range(hr):
            for c,h in zip(columns,headers):
                hh=h.split('\n')
                if len(hh)>i: h=hh[i]
                else:         h=''
                print h.ljust(l[c])+' |',
            print '\n|',
        for c in columns:
            print '-'*l[c]+' |',
        for row in table:
            print '\n|',
            for c in columns:
                if c not in row:                        print     '?'    .ljust(l[c])+' |',
                elif type(row[c]) in types.StringTypes: print     row[c] .ljust(l[c])+' |',
                else:                                   print str(row[c]).rjust(l[c])+' |',
        print

# interface for third party software to consume this script via python
def getLandscapeConfiguration(site):
    sysRepStatus = SystemReplicationStatus()
    return sysRepStatus.getLandscapeConfiguration(site)

class HSRTreeNode:
    def __init__(self, id="", name="", mode=""):
        self.id = id
        self.name = name
        self.mode = mode
        self.children = []
    def addChild(self, node):
        self.children.append(node)

def addToHSRTree(tree, hsrNodes, hsrMappings):
    for k, v in hsrMappings.items():
        if k == tree.id:
            for node in v:
                tree.addChild(hsrNodes[node])
                addToHSRTree(hsrNodes[node], hsrNodes, hsrMappings)

def printTree(tree, depth = 0):
    print "     |" * depth, ("---" if (depth > 0) else ""), tree.name, " (", tree.mode, ")"
    for child in tree.children:
        printTree(child, depth+1)

def printLandscapeTree():
    print "HANA System Replication landscape:"
    ns = NameServerPy.TNSClient()
    ownSiteId = ns.getDRDatacenter()

    hsrNodes = {}
    hsrMappings = {}

    # Site names
    names = NameServerPy.TNode()
    ns.getTree('/datacenters/name',names)
    for name in names.getNodes():
        hsrNodes[name.getName()] = HSRTreeNode(id=name.getName(), name=name.getValue())

    # Replication modes
    modes = NameServerPy.TNode()
    ns.getTree('/datacenters/mode',modes)
    for mode in modes.getNodes():
        hsrNodes[mode.getName()].mode = mode.getValue()

    # Create Mapping Tree
    mappings = NameServerPy.TNode()
    ns.getTree('/datacenters/mappings', mappings)
    for source in mappings.getNodes():
        hsrMappings[source.getName()] = []
        for target in source.getNodes():
            hsrMappings[source.getName()].append(target.getName())

    hsrTree = hsrNodes[str(ownSiteId)]
    addToHSRTree(hsrTree, hsrNodes, hsrMappings)
    printTree(hsrTree)

def main(argv):
    longFormat = False
    local = False
    site = None
    sapcontrol = False
    requestSecondaryActiveStatus = True

    if os.getuid() == 0:
        print "It is prohibited to execute systemReplicationStatus.py as root user"
        return 1

    syntaxHelp = 'systemReplicationStatus.py [-h|--help] [-a|--all] [-l|--localhost] [-s|--site=<site name>] [-t|--printLandscapeTree] [--omitSecondaryActiveStatus] [--sapcontrol=1]'
    try:
        opts,_ = getopt.getopt(argv, "hals:t",["help", "all", "localhost", "site=", "printLandscapeTree", "sapcontrol=", "omitSecondaryActiveStatus"])
    except getopt.GetoptError:
        print syntaxHelp
        return 2

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print syntaxHelp
            sys.exit(0)
        elif opt in ("-a", "--all"):
            longFormat = True
        elif opt in ("-l", "--localhost"):
            local = True
        elif opt in ("-s", "--site"):
            site = arg

        if opt in ("-t", "--printLandscapeTree"):
            printLandscapeTree()
            return 0

        if opt in ("--sapcontrol"):
            if arg == "1" or arg == 1:
                sapcontrol = True
        if opt in ("--omitSecondaryActiveStatus"):
            requestSecondaryActiveStatus = False

    sysRepStatus = SystemReplicationStatus()
    rc = sysRepStatus.getStatusAndPrint(longFormat, site, sapcontrol, requestSecondaryActiveStatus, local)

    return rc

if __name__ == "__main__":
    exitCode = main(sys.argv[1:])
    sys.exit(exitCode)
