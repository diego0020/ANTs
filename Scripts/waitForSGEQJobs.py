#!/usr/bin/python2

from __future__ import division

import sys
from itertools import izip
import subprocess
import datetime
import time
import os

# Usage: waitForSGEQJobs.py <verbose [1 or 0]> <delay in seconds in range 10-600> [job IDs] [job scripts]
#
#
# Takes as args a string of qsub job IDs, their associated scripts
# and periodically monitors them. Once they all finish, it returns 0
# Jobs that return an exit status different from zero are run again 
#
# If any of the jobs go into error state, an error is printed to stderr and the program waits for the non-error
# jobs to finish, then returns 1
#

# Usual qstat format - check this at run time
# job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID


# First thing to do is parse our input
ANTSPATH = os.environ.get("ANTSPATH","")
print ANTSPATH
reschedule_job = "qsub -cwd -S /bin/bash -N antsBuildTemplate_rigid -v ANTSPATH=%s -r yes %s | awk '{print $3}'"

args = sys.argv
print args

verbose = args[1] > 0
delay = int(sys.argv[2])

ids_and_names = args[3:]
ids=[int(i) for i in ids_and_names[:len(ids_and_names)//2]]
names=ids_and_names[len(ids_and_names)//2:]

job_scripts=dict(izip(ids,names))

# Check for user stupidity
if (delay < 10) :
    print "Sleep period is too short, will poll queue once every 10 seconds\n";
    delay = 10

elif (delay > 3600) :
    print "Sleep period is too long, will poll queue once every 60 minutes\n";
    delay = 3600


print "  Waiting for %d jobs: %s"%(len(ids),ids)

user=subprocess.check_output('whoami').strip()


qstatOutput = subprocess.check_output(('qstat', '-u', user))

if len(ids)==0 or len(qstatOutput)==0:
    # Nothing to do
    exit(0)


qstatLines = qstatOutput.split('\n')

header = qstatLines[0].strip().split()

# Position in qstat output of tokens we want
jobID_Pos = -1
statePos = -1

for i,s in enumerate(header) :
    if s == "job-ID":
        jobID_Pos = i
    elif s == "state" :
        statePos = i

# If we can't parse the job IDs, something is very wrong
if jobID_Pos < 0 or statePos < 0:
    print "Cannot find job-ID and state field in qstat output, cannot monitor jobs\n"
    exit(1)




# Now check on all of our jobs
jobsIncomplete = len(job_scripts)
completed_jobs = set()
failed_jobs = set()

while (jobsIncomplete>0) :
    sys.stdout.flush()
    time.sleep(delay)
    # Jobs that are still showing up in qstat
    qstatOutput = subprocess.check_output(('qstat', '-u', user))
    qstatLines = qstatOutput.split('\n')
    token_lines = (l.strip().split() for l in qstatLines[2:] if len(l) > 0)
    jobs_tuples = ((int(l[jobID_Pos]),l[statePos]) for l in token_lines)
    jobs_status = dict(t for t in jobs_tuples if t[0] in job_scripts)
    
    print "\n\n  (%s) Still waiting for %d jobs"%(datetime.datetime.now().ctime(),jobsIncomplete)
    if verbose:
        for j_i, j_s in jobs_status.iteritems():
            print "    Job %d is in state %s"%(j_i,j_s)

    

    possibly_completed_jobs = set(job_scripts.keys()) - set(jobs_status.keys()) 
    
    for j in possibly_completed_jobs:
        status = None
        while status is None:
            try:
                status=int(subprocess.check_output('qacct  -j %d | grep exit_status'%j , shell=True).strip().split()[1])
            except subprocess.CalledProcessError:
                time.sleep(10)
                
        if status == 0:
            #success            
            completed_jobs.add(j)
        else:
            failed_jobs.add(j)
            print "job %d failed, rescheduling job"%j
            new_job_id = int(subprocess.check_output(reschedule_job%(ANTSPATH,job_scripts[j]) , shell=True))
            job_scripts[new_job_id]=job_scripts[j]
            print "new job %d"%j
        job_scripts.pop(j)    
    
    print "successfully completed jobs:"
    print sorted(completed_jobs)
    print "failed jobs:"
    print sorted(failed_jobs)
    den = len(completed_jobs)+len(failed_jobs)
    if den==0:
        den = 1
    print "completed ratio: %.2f %%"%(len(completed_jobs)/den*100)
        
    jobsIncomplete = len(job_scripts)





print "  No more jobs in queue\n\n";
print "Failed jobs %s"%failed_jobs
exit(0)


