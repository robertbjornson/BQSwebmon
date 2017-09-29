import slurmBQS


def getSuspects(limit):
    info = slurmBQS.getInfo()

    J=info['jobs']
    N=info['nodes']
    suspects=[]

    for jid, j in J.items():
        l=0
        if j['hosts']:
            for nid, cnt in j['hosts'].items():
                n=N[nid]
                l+=float(cnt)/n['ncpus']*n['loadave']
            r=l/j['cores']
            if r < 0.25:
                suspects.append((j['cores'],r, jid, j))
    return sorted(suspects, reverse=True)

def findUsers(s):
    users={}
    for c, r, jid, j in s:
        users[j['owner']]=users.setdefault(j['owner'], 0)+c
    l=sorted([(c, u) for u, c in users.items()], reverse=True)
    return l

if __name__=='__main__':
    s=getSuspects(0.3)
    for c, r, jid, j in s:
        print "%d\t%f\t%s\t%20s\t%20s\t%s" %(c, r, jid, j['owner'], j['name'], j['hosts'])
    for c, u in findUsers(s):
        print "%d\t%s" % (c, u)
#        print "%20s%d" % (u, c)


    
