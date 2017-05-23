
import pdb, sys, pwd, re, itertools
import pyslurm
import collections

def convert_time (timestr):
    
    hours,mins,secs=timestr.split(':')

    seconds=(60*60*int(hours))+(60*int(mins))+int(secs)
    return seconds

# Expand Slurm's condensed nodelist syntax
# must handle
# c13n09,c14n[01-12],c16n[01,07],c17n[01-02,05],c18n[01-12],c19n[01-12],c20n[01-12],c21n[01-07,09-10],c22n[01-12],c23n[01-12],bigmem[01-03],gpu01,gpu[03-06]
# None

def parseNodeList(nls):
    idx=0
    tmpl=[]
    rngs=[]
    while idx < len(nls):
        c = nls[idx]
        if c == ',': # char concludes one entry, yield all the values                                                                                                              
            t=''.join(tmpl)
            if rngs:
                for rng in itertools.product(*rngs):
                    yield t % rng
            else:
                yield t
            tmpl=[]
            rngs=[]
            idx+=1
        elif c == '[': # bracket denotes start of a range.  Find end of range, generate list of it's values, and put a placeholder in the template.                                   
            tmpl.append("%s")
            e=nls.find(']',idx,len(nls))
            if e==-1:
                raise Exception("Hey, bad format")
            rss=nls[idx+1:e]
            idx=e+1
            lst=[]
            subfds = rss.split(',')
            for sf in subfds:
                if not '-' in sf:
                    lst.append(sf)
                else:
                    s, f = re.match('^(\d\d)-(\d\d)$', sf).groups()
                    nds=["%02d" % i for i in range(int(s), int(f)+1)]
                    lst+=nds
            rngs.append(lst)
        else: # just a plain character, add to template                                                                                                                               
            tmpl.append(c)
            idx+=1

    # handle last element                                                                                                                                                             
    r=''.join(tmpl)
    if rngs:
        for rng in itertools.product(*rngs):
            yield r % rng
    else:
        yield r

# process a string like this:
# u'tres_alloc_str': u'cpu=1,mem=4000M,node=1'
def parse_tres(s):
    d={}
    for e in s.split(','):
        k, v=e.split('=')
        d[k]=v
    return d

def mem2GB(s):
    if s[-1]=='K':
        return float(s[:-1])/(1024.0*1024)
    elif s[-1]=='M':
        return float(s[:-1])/1024.0
    elif s[-1]=='G':
        return float(s[:-1])
    else:
        return float(s)

#print list(pnl("c13n09,c14n[01-12],c16n[01,07],c17n[01-02,05],c18n[01-12],c19n[01-12],c20n[01-12],c21n[01-07,09-10],c22n[01-12],c23n[01-12],bigmem[01-03],gpu01,gpu[03-06]"))        

def getInfo():
    info={}
    info['nodes']=Ns={}
    info['jobs']=Js={}
    info['queues']=Qs={}
    try:
        sl_nodes=pyslurm.node().get()
        sl_jobs=pyslurm.job().get()
        sl_queues=pyslurm.partition().get()
    except Exception, e:
        print "<h3>Error connecting to Slurm:</h3><tt>",e,"</tt>"
        sys.exit(1)

    for sl_nn, sl_nd in sl_nodes.iteritems():
        Ns[sl_nn]=N={}
        # (list of strings) queues this node serves (filled in below)
        N['queues']=[]
        # (int) xcores on node
        N['ncpus']=sl_nd['cpus']
        # (float) node RAM in GB
        N['physmem']=sl_nd['real_memory']/1024.0 
        # (float) total RAM allocated by jobs in GB
        N['physmem']=sl_nd['alloc_mem']/1024.0 
        # (int) users on node 
        N['nusers']=0  #FIX
        # (float) node load average
        N['loadave']=float(sl_nd['cpu_load'])/100.0 # FIX
        # (list of strings) because some BQSs return multiple states
        tmp=sl_nd['state'].split('+')[0] 
        if tmp[-1]=='*': 
            tmp=tmp[:-1] # * means not responding; ignore for now
        N['state']=[tmp,]
        # dict holding node-specific info for each job (cores, memory used by this job on this node)
        # 'jobs': {'145': {"cores": 20, "mem": 6000}}
        N['jobs']={}
        # (int) total number of active cores on this node across all jobs
        N['activecores']=sl_nd['alloc_cpus']

    for sl_qn, sl_qd in sl_queues.iteritems():
        Qs[sl_qn]=Q={}
        # (string) desc of nodes using this queue: 'c[13-16,18-20]n[01-12]'
        # can be anything, current not used
        Q['prettynodes']=sl_qd['nodes']
        # (list of strings) expanded individual node list
        # ['c13n01', 'c13n02', 'c13n03', 'c13n04', 'c13n05']...
        Q['nodes']=list(parseNodeList(sl_qd['nodes']))

        # add this queue to the nodes assigned to it
        # (see above)
        for nn in Q['nodes']:
            if nn not in Ns: continue # temporary fix for situation where Q lists nodes node defined in nodes.
            Ns[nn]['queues'].append(str(sl_qn))

        # How many jobs in this queue are in each state:
        # Counter({'R': 1} 
        # filled in below
        Q['statecount']=collections.Counter()


    for sl_jid, sl_jd in sl_jobs.iteritems():
        sl_jid=str(sl_jid)
        Js[sl_jid]=J={}

        # Note jid is the internal 'name' of the job.  Normally an integer, but I'm using a string because
        # in some cases it is not an integer, e.g. torque job arrays.
        # The name fiels, on the other hand is the string provided by the user, and can be anything, and not unique.

        # (string) can be R, C, Q, etc.
        J['state']=sl_jd['job_state'][0]
        # (string) netid of job owner
        J['owner']=pwd.getpwuid(sl_jd['user_id']).pw_name
        # (string) job queue
        J['queue']=sl_jd['partition']
        # (string) job name
	# Total hack to avoid problems when name is unicode ??
	J['name']=sl_jd['name'].encode('punycode')
        # (dict) mapping node name to number of cores used for all nodes participating in this job
        #{'c13n08': 20}
        J['hosts']=sl_jd['cpus_allocated']
        # (int) total cores used by this job
        J['cores']=sl_jd['num_cpus']
        # (int) sec of cpu time used by this job # FIX
        J['cputime']=convert_time(sl_jd.get('resources_used',{}).get('cput', ['00:00:00'])[0]) # sec
        # (int) sec of wall time used by this job
        J['walltime']=sl_jd['run_time']
        # (float) GB of memory used by this job
        #J['mem']=parse_tres(sl_jd.get('tres_alloc_str', {})).get('mem', "?")

        tres=sl_jd['tres_alloc_str']
	# mem is total memory for this job, across all nodes
        if tres:
            J['mem']=mem2GB(parse_tres(tres).get('mem', 0))
        else:
            J['mem']=mem2GB(parse_tres(sl_jd['tres_req_str']).get('mem', '0'))

        # add this jobs's states to the queue count
        (Qs[J['queue']]['statecount'])[J['state']]+=1

        # add this job to the list of jobs on all of its nodes
        if J['state']=='R':
            for nn in J['hosts']:
                if not sl_jid in  Ns[nn]['jobs']:
                     Ns[nn]['jobs'][sl_jid]={}
                Ns[nn]['jobs'][sl_jid]["cores"]=J['cores']
                # here we calculate the memory footprint of the cores on this mode only
                # remember, J['mem'] is the total mem for the job across all nodes
                if sl_jd['mem_per_node']:
                    Ns[nn]['jobs'][sl_jid]["mem"]=J['mem']/sl_jd['num_nodes']
                elif sl_jd['mem_per_cpu']:
                    Ns[nn]['jobs'][sl_jid]["mem"]=J['mem']/sl_jd['num_cpus']*J['hosts'][nn]
                else:
                    error("huh")

    return info

info = getInfo()

print "JOBS"
for jid, jd in info['jobs'].iteritems():
    print jid, jd

print "NODES"
for nn, nd in info['nodes'].iteritems():
    print nn, nd

print "QUEUES"
for qn, qd in info['queues'].iteritems():
    print qn, qd

