#!/usr/bin/python
# Copyright (c) 2009 Stephen Childs, and
# Trinity College Dublin.
#
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Heavily modified for use at Yale by NJC.
#

## Config section
CLUSTER_NAME='Farnam'

import slurmBQS as BQS
## end of config section

from getopt import getopt
import grp, pwd, sys
import datetime
from time import strftime
import os,urllib,pdb

import cgitb; cgitb.enable()

import cStringIO

JOB_EFFIC={}
#JOB_STATES=['R','Q','E','C','H','W','T']

# slurm states
# PENDING,RUNNING,SUSPENDED,COMPLETED,CANCELLED,FAILED,TIMEOUT,NODE_FAIL,PREEMPTED,BOOT_FAIL,DEADLINE,COMPLETING,CONFIGURING,RESIZING,SPECIAL_EXIT
JOB_STATES=['R','Q','F', 'E','C','H','W','T']
# NODE_STATES=['down','free','job-exclusive','offline','state-unknown']
NODE_STATES=['DOWN', 'IDLE', 'MIXED', 'ALLOCATED']
REFRESH_TIME = "30"
USER_EFFIC={}

def user_effic(user):
    effic=0.0

    if user in USER_EFFIC:
        for job in USER_EFFIC[user]:
            effic=effic+job
        effic=(effic/float(len(USER_EFFIC[user]))*100.0)

    return effic
        

def job_effic(myjob):

    effic=0.0
    walltime=0.0
    if myjob.has_key('cputime'):
        cput=myjob['cputime']
        walltime=myjob['walltime']
        
    if walltime != 0.0:
        effic=float(cput)/float(walltime)

    return effic

def get_poolmapping(gridmapdir):

    # find files with more than one link in current directory
    allfiles=os.listdir(gridmapdir)

    maptable=dict()
    inodes=dict()

    # sort the list so the "dn-files" come first
    allfiles.sort()

    for file in allfiles:
        statinfo=os.stat(os.path.join(gridmapdir,file))

        if (file[0] == '%'):
            inodes[statinfo.st_ino]=urllib.unquote(file)
        else:
            if (statinfo.st_nlink == 2):
                maptable[file]=inodes[statinfo.st_ino]

    return maptable

def get_dn (ownershort):
    # user info
    if False and ownershort in userdnmap:
        ownerdn=userdnmap[ownershort]
    else:
        ownerdn=ownershort

    return ownerdn

def fill_user_list (jobs):
    users={}
    for name, atts in jobs.items():
        job_state = atts.get('state')
        ownershort = atts.get('owner')
        effic = job_effic(atts)
        if (job_state == "R"):
            USER_EFFIC.setdefault(ownershort, []).append(effic)
        user = users.setdefault(ownershort, {})
        user['Jobs'] = user.get('Jobs', 0) + 1
        # RDB the change I made below requires this
        user['Nodes'] = user.get('Nodes', set())
        # A bit of hack, but there is no obvious bit of info reporting the core count directly.
        cores = atts.get('cores')
        # RDB this was treating -- as a node name, and counting it in nodes and cores
        if (job_state =="R"):
            print sys.stderr, 'atts', atts['hosts']
            user['Nodes'].update(atts['hosts'].keys())
            user['Cores'] = user.get('Cores', 0) + atts.get('cores')
        user[job_state] = user.get(job_state, 0) + 1

    print sys.stderr, "user_list", users
    print sys.stderr, "USER_EFFIC", USER_EFFIC
    return users
           
def print_user_summary(users):

    sysso = sys.stdout
    sys.stdout = cStringIO.StringIO()

    print "<table class='example table-sorted-desc table-autosort:1 table-stripeclass:alternate user_summary' >"
    print "<caption>Users</caption>"
    print "<thead><tr>",
    print "<th class='table-filterable table-sortable:default' align='left'>User</th><th class='table-filterable table-sortable:default' align='left'>Group</th>"
    totals={}

    for state in ['Jobs', 'Nodes', 'Cores'] + JOB_STATES:
        print "<th class='table-filterable table-sorted-desc table-sortable:numeric'>%s</th>" % state
        totals[state]=0
    print "<th class='table-sortable:numeric'>Efficiency</th>"
    print "</tr></thead>"

    u2gs = {}
    for g, d, d, us in grp.getgrall():
        if g == 'portal': continue
        for u in us:
            u2gs.setdefault(u, []).append(g)

    nodes, total = set(), 0
    for user, atts in users.items():
        if 'Jobs' not in atts: continue

        total = total + atts['Jobs']
        nodes.update(atts['Nodes'])

        try:
            gs = u2gs.get(user, [])
            group = grp.getgrgid(pwd.getpwnam(user).pw_gid).gr_name
            if group != user: gs.append(group)
            try:
                gs.remove('portal')
            except:
                pass
            # some users have a lot of groups..
            MAXGRPS=3
            gs=sorted(gs)
            if len(gs) > MAXGRPS:
                group=','.join(gs[0:MAXGRPS]) + ",..."
            else:
                group = ','.join(sorted(gs))
            
        except:
            group = '???'
        print "<tr><td onMouseOver='highlight(\"%s\")' onMouseOut='dehighlight(\"%s\")'title='%s'>%s</td><td>%s</td>" % (user,user,get_dn(user),user,group)
        for state in ['Jobs', 'Nodes', 'Cores'] + JOB_STATES:
            if state == 'Nodes':
                print >> sys.stderr, "GAH", atts
                c = len(atts.get(state, []))
            else:
                c = atts.get(state, 0)
            print "<td>%d</td>" % c
            totals[state]=totals[state]+c
        print "<td>%.0f</td>" % user_effic(user)
        print "</tr>"

    totals['Nodes'] = len(nodes) # some user may share nodes, so doing this avoids double counting.
    print "<tfoot><tr><td><b>Total</b></td><td/>"
    for state in ['Jobs', 'Nodes', 'Cores'] + JOB_STATES:
        print "<td>%s</td>" % totals[state]
    print "</tr>"
    print '''</tfoot>'''
    print "</table>"
    buf = sys.stdout.getvalue()
    sys.stdout.close()
    sys.stdout = sysso
    return buf

def print_node_summary(nodes):
    sysso = sys.stdout
    sys.stdout = cStringIO.StringIO()

    print "<table class='example table-sorted-desc table-autosort:1 node_summary'>"
    print "<caption>Nodes</caption>"
    print "<thead><tr>",

    totals={}

    print "<th class='table-filterable table-sortable:default' align='left'>State</th>"
    print "<th class='table-filterable table-sorted-desc table-sortable:numeric'>Count</th>"
    nodes_in_state={}
    for s in NODE_STATES:
        totals[s]=0
        nodes_in_state[s]=[]


    print "</tr></thead>"
    for name, node in nodes.items():
        if 'state' in node.keys():
            s=node['state']
            s=s[0].split(',')[0]
	    totals[s]=totals[s]+1
            nodes_in_state[s].append(name)

    total=0
    s=s.split(',')[0]
    for s in NODE_STATES:
        tdclass=s
        (nodes_in_state[s]).sort()
	nodes_str=' '.join(nodes_in_state[s])
        print "<tr><td class='%s' title='%s'>%s</td><td class='%s'>%d</td></tr>" %(tdclass,nodes_str,s,tdclass,totals[s])
        total=total+totals[s]
    print "<tfoot><tr><td><b>Total</b></td><td>%d</td></tr></tfoot>" %total
    print "</table>"
    buf = sys.stdout.getvalue()
    sys.stdout.close()
    sys.stdout = sysso
    return buf


def print_queue_summary(info):
    sysso = sys.stdout
    sys.stdout = cStringIO.StringIO()

    print "<table class='example table-sorted-desc table-autosort:1 table-stripeclass:alternate queue_summary'>"
    print "<caption>Queues</caption>"

    print "<thead><tr>"
    headers=['R','Q','H', 'Avail']
    print "<th class='table-filterable table-sortable:default' align='left'>Name</th>"
    totals={}
    for h in headers:
        totals[h]=0
    
    for header in headers:
        print "<th class='table-filterable table-sortable:numeric'>",header,"</th>"
    print "</tr></thead>"
    nond = []
    for queue, atts in info['queues'].items():
        print "<tr>",
        print "<td>",queue, "</td>",

        statedict=atts['statecount']

        for s in headers:
            if s == 'Avail': continue # hack
            print "<td align='right'>",statedict[s],"</td>",
            totals[s]=totals[s]+int(statedict[s])

        c2avail, summary = {}, []

        for n in atts['nodes']:
            try:
                nd = info["nodes"][n]
                print >> sys.stderr, "nd", nd
            except:
                nond.append(n)
                continue
            cores = int(nd['ncpus'])
            d = c2avail.setdefault(cores, {})
            print >> sys.stderr, n, 'state', nd['state']
            if ('DOWN' in nd['state']) or ('DOWN*' in nd['state']): #FIX
                d['Down'] = d.get('Down', 0) + 1
            elif 'offline' in nd['state']:
                d['offline'] = d.get('offline', 0) + 1
            else:
                #avail = cores - len(nd.get('jobs', [])) 
                avail = cores-nd['activecores']
                d[avail] = d.get(avail, 0) + 1
        print >> sys.stderr, 'c2avail', c2avail
        for c in sorted(c2avail.keys()):
            d = c2avail[c]
            extras = []
            if 'Down' in d:
                cc = d.pop('Down')
                extras.append('%d/D'%cc)
            if 'offline' in d:
                cc = d.pop('offline')
                extras.append('%d/F'%cc)
            summary.append('%dC&nbsp;'%c + ','.join(['%d/%d'%(d[a], a) for a in sorted(d.keys())]+extras))
        print '<td>%s</td>'%'<br>'.join(summary)

        print "</tr>"
    print "<tfoot><tr><td><b>Total</b></td>",
    for h in headers:
        if h == 'Avail':
            print "<td></td> "
        else:
            print "<td align='right'>%d</td> " %(totals[h])
    print "</tr></tfoot>"
    print "</table>"
    if nond:
        print 'No node data for: '+','.join(nond)
    buf = sys.stdout.getvalue()
    sys.stdout.close()
    sys.stdout = sysso
    return buf

def print_key_table():
    sysso = sys.stdout
    sys.stdout = cStringIO.StringIO()

    print "<table class='key_table'>"
    print "<tr><th>Node color codes</th></tr>"
    allstates=NODE_STATES[:]
    allstates.append('partfull')
    for s in allstates:
        print "<tr><td class='%s'>%s</td></tr>" %(s,s)

    print "</table>"
    buf = sys.stdout.getvalue()
    sys.stdout.close()
    sys.stdout = sysso
    return buf
    
'''                                                                                                                                                                                   
RDB9                                                                                                                                                                                  
                                                                                                                                                                                      
Accepts a list that looks like:                                                                                                                                                       
['8-15/943580.rocks.louise.hpc.yale.internal', '0-7', '8-10/945340.rocks.louise.hpc.yale.internal']                                                                                   
and adds them to the dict                                                                                                                                                             
{'945340.rocks.louise.hpc.yale.internal':11, '943580.rocks.louise.hpc.yale.internal':8}                                                                                               
'''

def rng2cnt(c):
    cc=c.split('-')
    if len(cc) == 1:
        return 1
    elif len(cc) == 2:
        return int(cc[1])-int(cc[0])+1

def jl2cnt(jl):
    d={}
    sum=0
    for j in jl:
        tmp=j.split('/')
        if len(tmp)==1:
            # only got core #s                                                                                                                                                        
            sum+=rng2cnt(tmp[0])
        else:
            corelist, jobname=tmp
            sum+=rng2cnt(corelist)
            d[jobname]=d.get(jobname, 0)+sum
            sum=0
    return d

'''                                                                                                                                                                                   
This is a bit tricky now.                                                                                                                                                             
PBSQuery doesn't parse exec_host attribute of a job correctly.  It splits on commas instead of +s.                                                                                    
For now, I am undoing the bad parsing, and reparsing correctly to get the number of cores in a job.                                                                                   
                                                                                                                                                                                      
'exec_host': ['compute-9-1/0', '2-3', '5+compute-9-8/0-7+compute-9-10/0-3']                                                                                                           
                                                                                                                                                                                      
'''
def job2cores(job):
    tmpstr=",".join(job['exec_host'])

    cnts={}
    for onenode in tmpstr.split('+'):
        nn, cores=onenode.split('/')
        for cc in cores.split(','):
            cnts[nn]=cnts.get(nn, 0)+rng2cnt(cc)

    #print "Turned %s into %s" % (job['exec_host'], str(cnts))                                                                                                                        
    return cnts    

GRID_COLS=4

'''
try:
    p=PBSQuery()
    nodes=p.getnodes()
    jobs=p.getjobs()
    queues=p.getqueues()
    del p #rdb9
except Exception, e:
    print "<h3>Error connecting to PBS server:</h3><tt>",e,"</tt>"
    sys.exit(1)
'''

info = BQS.getInfo()

users=fill_user_list(info['jobs'])
print sys.stderr, "user_list", users
print sys.stderr, "HERE"

DateTime = strftime("%Y-%m-%d %H:%M:%S")

UserSummary = print_user_summary(users)

QueueSummary = print_queue_summary(info)

NodeSummary = print_node_summary(info["nodes"])

sysso = sys.stdout
sys.stdout = cStringIO.StringIO()

count=0
#nodelist = [fn for c, b, fn in sorted([[int(c), int(b), fn] for fn in nodelist for d, c, b in [fn.split('-')]])]
#The new naming convention is simpler
#nodelist=sorted(nodelist)


for nn in sorted(info['nodes'].keys()):
    nd=info['nodes'][nn]
    node_state = nd['state'][0]
    print >> sys.stderr, 'node_state', nn, node_state
    myjobs = nd['jobs']

    loadave=nd['loadave']
    nusers = nd['nusers']
    physmem = nd['physmem']

    # FIX
    # define cell bg color based on state
    if (node_state == 'DOWN*'):
        node_state = 'DOWN'
    if (node_state == 'free' and (len(myjobs)>0)):
        node_state='partfull'
    if (node_state == 'down,job-exclusive'):
        node_state='down'
    print "<td valign='top'>" 
#                print "<b>%s</b>" %name
    queue = 'default'
    queue = nd['queues']


    print '''<form class='%s'><b>%s</b> %s
<INPUT class='job_indiv' TYPE="CHECKBOX" NAME="%s" checked="CHECKED" onClick="show_hide_data_id('%s',\
this.checked);" /><span style="font-size:10pt">Show jobs''' % (node_state, nn, queue, nn, nn)
    print "<br>%d jobs, %d cores, %s users, %.2f GB, %s load</span></form>" % (len(myjobs), nd['activecores'],nusers,physmem,loadave)
    print "<span class='jobdata' id='"+nn+"' style='display:block'>"
    
    for jid in sorted(myjobs.keys()):
        print >> sys.stderr, "myjobs", myjobs
        print >> sys.stderr, "jobs", info["jobs"]
        myjob=info["jobs"][jid]

        owner=myjob['owner']

        cput = myjob['cputime']
        print >> sys.stderr, 'myjob', myjob
        numCpusOnNode = myjob['hosts'][nn]
        mem = nd['physmem']
        # FIX
        memreq = mem
        walltime = myjob['walltime']

        myqueue = myjob['queue']

        print "<span title='"+jid+": "+myqueue+"'>"+str(numCpusOnNode)+ ": "+jid+ "</span>",

        # user info
        ownerdn=get_dn(owner)

        print "<span class= '%s' title='%s'> %-9s</span>" %(owner,ownerdn,owner),

        print "<span title='%s/%s s'>" % (cput, walltime),
        effic=0.0

        if 0.0 == walltime:
            effic = -1.0
            print "<font color='blue'>",
        else:
            # is this more of a hack then parsing and interpretting a string like "10:ppn=8"?
            numCpus = len(myjob['hosts']) # RDB FIX need to resolve this
            effic = float(cput)/(numCpus * float(walltime))
            if effic < .8:
                print "<font color='gray'>",
            else:
                if effic > 1.0:
                    print "<font color='red'>",
                else:
                    print "<font color='black'>",

        print "%7.2f%%</font> " % (effic*100.0),
        print "</span>",


        if mem > memreq and memreq > 0.0:
            print "<font color='red'>",
        else:
            if mem < 0.5*memreq:
                print "<font color='gray'>",
            else:
                print "<font color='black'>",


        print "%.2f/%.2f GB</font>" %(mem,memreq)
    print "</span>"
    print "</td>"
    if (count and ((count%GRID_COLS))==GRID_COLS-1):
        print "<!-- ",count,"!-->\n"
        print "</tr>\n<tr>\n"
    count=count+1

NodeTable = sys.stdout.getvalue()
sys.stdout.close()
sys.stdout = sysso

sysso = sys.stdout
sys.stdout = cStringIO.StringIO()

for name,job in info['jobs'].iteritems():
    print "<tr>"
    print "<td>",name,"</td>"
    print "<td>",job['owner'],"</td>"
    print "<td>",job['queue'],"</td>"
    print "<td>",job['name'],"</td>"
    if job['state'] == 'R' and job.has_key('hosts'): # Hack to avoid rogue jobs
        cores = job['cores']
        nodes = len(job['hosts'])
        print "<td>%d</td><td>%d</td>"%(nodes, cores)
    else:
        print '<td></td><td></td>'
    print "<td>",job['state'],"</td>"
    try:
        walltime = job['walltime']
        print "<td>",walltime,"</td>"
    except:
        print "<td></td>"

    print "</tr>"

JobTable = sys.stdout.getvalue()
sys.stdout.close()
sys.stdout = sysso

from twisted.web.resource import Resource

class PBSMonPage(Resource):
    isLeaf = True
    def render_GET(self, request):
        return open('pbsWebMon.html').read()%globals()

resource = PBSMonPage()
