" Insert credentials into mrjob configuration file "
import sys, os, pickle

if 'EC2_VAULT' in os.environ.keys():
    vault=os.environ['EC2_VAULT']
    print vault
else:  # If EC2_VAULT is not defined, we assume we are in an EC2 instance
    vault='/home/ubuntu/Vault'
try:
    with open(vault+'/Creds.pkl') as file:
        Creds=pickle.load(file)
    print 'Creds=',Creds
    keypair=Creds['mrjob']
    print 'keypair=',keypair
    template=open('mrjob.conf.template').read()
    print "start filling"
    print "*"*10
    print template
    print "*"*10
    filled= template % (keypair['ID'],keypair['key_id'],keypair['secret_key'],keypair['s3_logs'],keypair['s3_scratch'])
    print "*"*10
    print filled
    print "*"*10
    home=os.environ['HOME']
    outfile = home+'/.mrjob.conf'
    open(outfile,'wb').write(filled)
    print 'Created the configuration file:',outfile
except Exception, e:
    print e


