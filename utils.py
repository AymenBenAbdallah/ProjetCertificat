import boto3
import botocore
import urllib.request
from fabric import Connection
import os
import time
import paramiko

ec2_resource = boto3.resource('ec2')
ec2_client = boto3.client('ec2')
ssm_client = boto3.client("ssm")

path = os.getenv("HOME") + "/.CertifProjet"

def get_ext_ip():
    '''
    Retrieve external IP of this machine.
    Useful to allow SSH inbound access to cluster only from this device.
    '''
    try:
        ip = urllib.request.urlopen('http://checkip.amazonaws.com').read().decode('utf-8').strip()
        print(f"Your public IP address is {ip}. This will be "
          f"used to grant SSH access to the Amazon EC2 instance.")
    except:
        print("ERROR : Couldn't retrieve external IP !")
        raise
    else:
        return ip

def get_image_id(name):
    '''
    Retrieve the id of the image used to create instances.
    :param name: the exact name of the image.
    :return: the image id.
    '''
    try:
        image_id = ssm_client.get_parameter(Name=name)['Parameter']['Value']
        if (len(image_id) == 0):
            raise AssertionError
    except:
        print("ERROR : Couldn't retrieve Ubuntu 20.04 image id !")
        raise AssertionError
    else:
        return image_id

def get_master(vm_instances):
    '''
    Gather EC2 master instance metadata.
    :param vm_instances: the instances of CertifProjet.
    :return: master instance metadata.
    '''
    for instance in vm_instances:
        for tag in instance.tags:
            if tag['Key'] == 'Name' and tag['Value'] == "CertifProjet-inst-0":
                return instance

def get_slaves(vm_instances):
    '''
    Gather EC2 slaves instances metadata.
    :param vm_instances: the instances of CertifProjet.
    :return: slave instances metadata.
    '''
    slaves = []

    for instance in vm_instances:
        for tag in instance.tags:
            if (
                tag['Key'] == 'Name' and
                tag['Value'].startswith('CertifProjet-inst-') and
                tag['Value'] != "CertifProjet-inst-0"
            ):
                slaves.append(instance)

    return slaves

def connect_ssh(instance_ip):
    """def ssh_connect_with_retry(ip_address):
        privkey = paramiko.RSAKey.from_private_key_file(
            '/home/aymen/Downloads/momo_keypair.pem')
        interval = 5
        ssh = paramiko.SSHClient()
        try:
            print('SSH into the instance: {}'.format(ip_address))
            ssh.connect(hostname=ip_address,
                        username='ec2-user', pkey=privkey)
            return ssh
        except Exception as e:
            print(e)"""
    # Get the private key of the CBD Project cluster
    key_path = os.path.expanduser(path + "/CertifProjet-key.pem") 

    # Here 'ubuntu' is user name and 'instance_ip' is public IP of EC2
    nodeClient = Connection(
    host=instance_ip, 
    user="ubuntu", 
    connect_kwargs={
        "key_filename": key_path,
        },
    )

    # Connect/ssh to an instance
    try:
        nodeClient.open()
        if nodeClient.is_connected:
            print(f"SSH session established with host unbuntu@{ instance_ip }")
        else:
            raise NameError("SessionError")
    except NameError:
        print("SSH session can't be established")
        raise

    return nodeClient

def get_running_instances():
    '''
    Fetch and extract instances from CertifProjet.
    Instances must be running to be stopped.
    :return: The running instances from CertifProjet.
    '''
    running_instances = ec2_resource.instances.filter(Filters=[
        {'Name': 'instance-state-name', 'Values': ['running']},
        {'Name': 'tag:Name', 'Values': ['CertifProjet*']}
    ])

    return running_instances

def get_subnet():
    '''
    Fetch the private subnet of CertifProjet.
    :return: The private subnet of CertifProjet.
    '''
    subnet = ec2_client.describe_subnets(Filters=[{
        'Name': 'tag:Name',
        'Values': ['CertifProjet-subnet']
    }])['Subnets'][0]

    return subnet

def get_vpcs():
    '''
    Fetch VPCs of CertifProjet.
    :return: The VPCs from CertifProjet.
    '''
    vpcs = ec2_client.describe_vpcs(Filters=[{
        'Name': 'tag:Name',
        'Values': ['CertifProjet*']
    }])['Vpcs']

    return vpcs

def get_security_groups():
    '''
    Fetch security groups of CertifProjet.
    :return: The security groups from CertifProjet.
    '''
    security_groups = ec2_client.describe_security_groups(Filters=[{
        'Name': 'group-name',
        'Values': ['CertifProjet*']
    }])

    return security_groups

def get_key_pairs():
    '''
    Fetch key pairs of CertifProjet.
    :return: The key pairs from CertifProjet.
    '''
    key_pairs = ec2_client.describe_key_pairs(Filters=[{
        'Name': 'key-name',
        'Values': ['CertifProjet*']
    }])['KeyPairs']

    return key_pairs

def hostname_resolution(ssh_client, master, slaves):
    """
    Edit /etc/hosts to add hostname resolution of all instances for a specific ssh_client.
    :param ssh_client: The SSH client of the targeted instance.
    :param master: The master instance metadata.
    :param slaves: The slaves instance metadata.
    """
    ssh_client.run(f"echo '{ master.private_ip_address } master' | sudo tee -a /etc/hosts", hide='stdout')
    n = 0
    # Hostname resolution of slaves
    for slave in slaves:
        ssh_client.run(f"echo '{ slave.private_ip_address } slave{n}' | sudo tee -a /etc/hosts", hide='stdout')
        n+=1

def setup_network(vm_instances):
    """
    Create a pair of SSH keys on the master and copy the public key on each slave.
    Also sets up the hostname resolution on every nodes.
    :param vm_instances: the instances of CertifProjet.
    """
    master = get_master(vm_instances)
    slaves = get_slaves(vm_instances)

    masterClient = connect_ssh(master.public_ip_address)

    # Setup master configuration
    #   Hostname configuration
    masterClient.sudo("hostnamectl set-hostname master")
    hostname_resolution(masterClient, master, slaves)    

    #   SSH configuration
    masterClient.run("ssh-keygen -q -t rsa -f ~/.ssh/cluster-key -P ''")
    result = masterClient.run("cat ~/.ssh/cluster-key.pub | sudo tee -a ~/.ssh/authorized_keys", hide='both')
    cluster_pub_key = result.stdout
    print("\tGenerated a key pair for intra-cluster communication.")

    masterClient.run("echo '    PubkeyAuthentication yes\n" +
                        "    IdentityFile ~/.ssh/cluster-key\n    StrictHostKeyChecking accept-new' " +
                        "| sudo tee -a /etc/ssh/ssh_config", hide='both')
    masterClient.sudo("/etc/init.d/ssh reload", hide='both')
    masterClient.close()
    print(f"SSH session to host unbuntu@{ master.public_ip_address } closed")

    # Setup slaves configuration
    for i in range(len(slaves)):
        slaveClient = connect_ssh(slaves[i].public_ip_address)
        slaveClient.sudo(f"hostnamectl set-hostname slave{i}")
        hostname_resolution(slaveClient, master, slaves)
        print("\tCopying cluster public key...", end='', flush=True)
        slaveClient.run(f"echo '{cluster_pub_key}' | sudo tee -a ~/.ssh/authorized_keys", hide='both')
        print("Done")
        slaveClient.close()
        print(f"SSH session to host unbuntu@{ slaves[i].public_ip_address } closed")
    print("Successfully copied public key on slaves.")