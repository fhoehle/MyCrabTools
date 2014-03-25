import os,sys,subprocess,re
#####
class crabDeamon(object):
  def __init__(self,autoFind = True):
    if autoFind:
      self.findCrabJobDir() 
    else: self.crabJobDir = None
  def findCrabJobDir(self,where=os.getenv('PWD')):
    import os
    possCrabs = os.walk(where).next()[1];possCrabs.sort()
    self.crabJobDir = (where +(os.path.sep if not where.endswith(os.path.sep) else "")+possCrabs[0]) if len (possCrabs) > 0 else None
    if not self.crabJobDir.startswith('crab_0_'):
      print "Warning using not crab default directory ",self.crabJobDir
  def executeCommand(self,command,debug = False,returnOutput = False,where = ''):
    if not hasattr(self,'crabJobDir') and not '-create' in command:
      print "no crabJobDir given "; return
    import subprocess,os,sys
    whereExec = ""
    if not '-create' in command:
      whereExec = "-c "+self.crabJobDir+" " 
    stopKey = 'stopKeyDONE'
    command = ("cd "+where+" &&" if where else "")+ " crab "+whereExec +command+' ;echo "returnCodeCrab: "$?"!"; echo "'+stopKey+'"'
    if debug:
      print "executing command ",command
    subPrOutput = subprocess.Popen([command],bufsize=1 , stdin=open(os.devnull),shell=True,stdout=subprocess.PIPE,env=os.environ)
    subPStdOut = [];crabExitCode=None
    for line in iter(subPrOutput.stdout.readline,stopKey+'\n'):
      if 'returnCodeCrab' in line:
        crabExitCode=line
      if debug:
        print line,
      subPStdOut.append(line)
    print ""
    subPrOutput.stdout.close()
    crabExitCode = re.match('returnCodeCrab:\ ([^!]*)!',crabExitCode).group(1) if re.match('returnCodeCrab:([^!]*)!',crabExitCode) else None
    if subPrOutput.poll() == None:
      os.kill(subPrOutput.pid,9)
    if not returnOutput:
      if debug:
        print "Crab ExitCode ",crabExitCode
      return crabExitCode
    else:
      return subPStdOut
  def status(self):
    self.executeCommand("-status",debug = True)
  def getoutput(self):
    self.executeCommand("-getoutput",debug = True)
  def multiCommand(self,command,listJobs,debug=False):
    numJobs = len(listJobs)
    crabCommand = None; exitCode = ""
    for i in range(numJobs/500+1):
      begin=(i*500); end=(((i+1)*500) if ((i+1)*500) < numJobs else numJobs)
      jobsToSubmit = listJobs[begin:end]
      crabCommand=command+" "+",".join(jobsToSubmit)
      exitCode = self.executeCommand(crabCommand,debug,False)
      if exitCode != "0":
        print 'command ',crabCommand," failed with ",exitCode
  def jobRetrievedGood(self):
    return self.testJobStatus( [(2,"Retrieved"),(5,"0")],self.getStatusList())
  def getStatusList(self):
    import re
    self.executeCommand("-status")
    completeOutput = self.executeCommand("-status",False,True)
    return [ l for l in completeOutput if re.match('^[0-9]+[ \t]+[YN][ \t]+[a-zA-Z]+[ \t]+',l)]
  def testJobStatus(self,tests,statusOutputs):
    noTest=len(tests)
    maxP = max( [ t[0] for t in tests])   
    return [ statusOutput.split()[0] for statusOutput in statusOutputs if maxP < len(statusOutput.split()) and sum( [statusOutput.split()[i] == t for i,t in tests]  ) == noTest ]
  def automaticResubmit(self,onlySummary = False,debug = False):
    jobOutput = self.getStatusList()
    doneJobsGood = self.jobRetrievedGood(); doneJobsBad = []; abortedJobs = []; downloadableJobs = []; downloadedJobsBad = [];downloadableNoCodeJobs=[]; createdJobs = [];
    for j in jobOutput:
      jSplit = j.split()
      if  len(jSplit) > 5:
        if jSplit[2] == "Retrieved" and jSplit[5] != "0":
          downloadedJobsBad.append(jSplit[0])
        if jSplit[2] == "Done" and jSplit[5] != "0":
          doneJobsBad.append(jSplit[0]) 
      if jSplit[2] == "Aborted":
        abortedJobs.append(jSplit[0])
      if len(jSplit) > 5 and jSplit[2]  == "Done" and jSplit[3]  == "Terminated" and jSplit[5] == "0": 
        downloadableJobs.append(jSplit[0])
      if  len(jSplit) < 6 and len(jSplit) > 2:
        if  jSplit[2]  == "Done" and jSplit[3]  == "Terminated" :
          downloadableNoCodeJobs.append(jSplit[0])
        if jSplit[3] == "Created" and jSplit[2] == "Created":
          createdJobs.append(jSplit[0])
    if not onlySummary:
      print " downloadableJobs ",downloadableJobs
      print "doneJobsGood ",doneJobsGood," ",len(doneJobsGood)," of ",len(jobOutput)
      print "doneJobsBad ", doneJobsBad 
      print "abortedJobs ",abortedJobs
      print "downloadedJobsBad ",downloadedJobsBad
      print "downloadableNoCodeJobs", downloadableNoCodeJobs
      print "back to Created  ",createdJobs
    if len(doneJobsBad+downloadableJobs+downloadableNoCodeJobs):
      exitCode = self.executeCommand("-get "+",".join(doneJobsBad+downloadableJobs+downloadableNoCodeJobs),debug = True and not onlySummary,returnOutput = False)
      if exitCode != "0" and not exitCode == None:
        sys.exit('getting outputFailed for bad Jobs'+",".join(doneJobsBad+downloadableJobs+downloadableNoCodeJobs)+' exitcode '+exitCode)
    if len(doneJobsBad+downloadedJobsBad+abortedJobs) > 0:
      self.multiCommand('-resubmit',doneJobsBad+downloadedJobsBad+abortedJobs,debug = True and not onlySummary)
    if len(createdJobs) > 0:
      self.multiCommand('-submit',createdJobs,debug = True and not onlySummary)
    if onlySummary:
      print "CrabJob ",self.postfix
      print "jobs done/retrieved good ",",".join(doneJobsGood+downloadableJobs)
      print len(doneJobsGood+downloadableJobs),"/",len(jobOutput)
      print "jobs resubmitted ",",".join(doneJobsBad+downloadedJobsBad+abortedJobs)
      print "just downloaded ",",".join(downloadableNoCodeJobs)
      print "back to Created  ",",".join(createdJobs)
  
