import os
import time
import utils
from botocore.exceptions import ClientError

# Number of instances to create (at least 2) 
nb_instances = 3
# Private subnet range
private_subnet = '10.0.0.0/24'
# The type of instance to use
instance_type = "t2.medium"

def create_key_pair():
    path = os.getenv("HOME") + "/.CertificatProjet"

    try:
        os.mkdir(path)
    except OSError:
        print ("Creation of the directory %s failed" % path)
    else:
        print ("Successfully created the directory %s" % path)

    try:
        key_pair = utils.ec2_resource.create_key_pair(KeyName="CertifProjet-key")
        with open(os.path.expanduser(f"{path}/CertifProjet-key.pem"), 'w') as pem_file:
            pem_file.write(key_pair.key_material)
            os.chmod(pem_file.name, 0o400)
            print(f"Wrote private key to {pem_file.name}.")
        return key_pair
    except ClientError:
        print("Couldn't create key cbdproject-key.")
        raise

def create_vpc():
    """
    Create a virtual private cloud (VPC) for CBD Project.

    :return: The newly created VPC and subnet.
    """

    # create VPC
    vpc = utils.ec2_resource.create_vpc(CidrBlock=private_subnet)
    vpc.wait_until_exists()
    # assign a name to the VPC
    vpc.create_tags(Tags=[{"Key": "Name", "Value": "cbdproject-vpc"}])

    # enable public dns hostname so that we can SSH into it later
    vpc.modify_attribute(EnableDnsSupport= { 'Value': True })
    vpc.modify_attribute(EnableDnsHostnames= { 'Value': True })

    # create an internet gateway and attach it to VPC
    ig = utils.ec2_resource.create_internet_gateway()
    vpc.attach_internet_gateway(InternetGatewayId=ig.id)

    # create a route table and a public route
    routetable = vpc.create_route_table()
    route = routetable.create_route(DestinationCidrBlock='0.0.0.0/0', GatewayId=ig.id)

    # create a subnet in the VPC
    subnet = utils.ec2_resource.create_subnet(CidrBlock=private_subnet, VpcId=vpc.id)
    subnet.create_tags(Tags=[{"Key": "Name", "Value": "cbdproject-subnet"}])
    routetable.associate_with_subnet(SubnetId=subnet.id)

    return vpc, subnet

def create_sg(vpc):
    """
    Create a security group in the CBD Project virtual private cloud (VPC)
    with inbound HTTP(S) and SSH access.

    :param vpc: The VPC used for the security group.
    :return: The newly created security group.
    """

    # Create security group
    try:
        sg = {
            'name': f"cbdproject-{str(time.time_ns())}",
            'description': "SG for N7 Cloud x Big Data project"
        }
        security_group = vpc.create_security_group(GroupName=sg['name'], Description=sg['description'])
        print(f"Created security group {sg['name']} in VPC {vpc.id}.")
    except ClientError:
        print(f"Couldn't create security group {sg['name']}.")
        raise

    # Get external IP for inbound SSH access
    try:
        ssh_cidr_ip = f"{utils.get_ext_ip()}/32"
    except:
        print("WARNING : Authorizing SSH from any device ! This could be dangerous...")
        ssh_cidr_ip = "0.0.0.0/0"

    # Define access policy for previously created security group
    try:
        ip_permissions = [
            {
                # HTTP ingress open to anyone
                'IpProtocol': 'tcp', 'FromPort': 80, 'ToPort': 80,
                'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
            },
            {
                # HTTPS ingress open to anyone
                'IpProtocol': 'tcp', 'FromPort': 443, 'ToPort': 443,
                'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
            },
            {
                # SSH ingress open to only the machine's external IP address
                'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22,
                'IpRanges': [{'CidrIp': f'{ssh_cidr_ip}'}]
            },
            {
                # Full open inside private network
                'IpProtocol': '-1',
                'UserIdGroupPairs': [{
                    'GroupId': security_group.id
                }],
            },
            {
                # HTTP ingress for Spark open to only the machine's external IP address
                'IpProtocol': 'tcp', 'FromPort': 50070, 'ToPort': 50070,
                'IpRanges': [{'CidrIp': f'{ssh_cidr_ip}'}]
            },
        ]
        security_group.authorize_ingress(IpPermissions=ip_permissions)
        print(f"""Set inbound rules for {security_group.id} to allow all inbound HTTP and HTTPS
        but only {ssh_cidr_ip} for SSH.
        """)
    except ClientError:
        print(f"ERROR : Couldnt authorize inbound rules for {sg['name']}.")
        raise
    else:
        return security_group    

def create_instance(image_id, instance_name, key_name, subnet_id, security_group_id):
    """
    Creates a new Amazon EC2 instance. The instance automatically starts immediately after
    it is created.
    :param image_id: The Amazon Machine Image (AMI) that is used to create the
                         instances.
    :param instance_name: The name of the instance.
    :param key_name: The name of a local file that contains the private key
                          that is used to connect to the instances using SSH.
    :param subnet_id: The subnet ID to associate to the instance.
    :param security_group_id: The security group ID to associate to the instance.

    :return: The newly created instance.
    """

    # Create instance
    try:
        instance = utils.ec2_resource.create_instances(
            ImageId=image_id,
            InstanceType=instance_type,
            KeyName=key_name,
            MinCount=1,
            MaxCount=1,
            NetworkInterfaces=[
                { 'SubnetId': subnet_id,
                    'DeviceIndex': 0,
                    'AssociatePublicIpAddress': True,
                    'Groups': [security_group_id]
                }],
            TagSpecifications=[
                { "ResourceType": "instance",
                    "Tags": [
                        {"Key": "Name",
                        "Value": instance_name}
                            ]
                }]
        )[0]
    except ClientError:
        print(f"Couldn't create instance with image {image_id}, instance type t2.micro, and key {key_name}.")
        raise
    else:
        return instance

def create_cluster():
    """
    Sets up prerequisites and creates instances used in the project.
    When this function returns, the instances are running and ready to use.
    """
    # Get current external IP
    current_ip_address = utils.get_ext_ip()

    # Retrieve Ubuntu 20.04 LTS amd64 image id
    image_id = utils.get_image_id("/aws/service/canonical/ubuntu/server/20.04/stable/current/amd64/hvm/ebs-gp2/ami-id")

    # Create key pair
    key_pair = create_key_pair()
    print(f"Created a key pair {key_pair.key_name} and saved the private key to ~/.cbdproject/{key_pair.key_name}.pem")

    # Create VPC and subnet
    vpc, subnet = create_vpc()
    vpc.wait_until_available()
    print(f"Created a VPC with ID {vpc.id} and subnet {subnet.cidr_block}")

    # Create security group
    sec_group = create_sg(vpc)
    print(f"Created security group {sec_group.group_name} that allows SSH, HTTP and HTTPS access from {current_ip_address}.")

    vm_instances = {}

    print("Creating instances, please wait...")

    for i in range(nb_instances):
        vm_instances[f'instance_{i}'] = create_instance(image_id, f"cbdproject-inst-{i}", key_pair.key_name, subnet.id, sec_group.group_id)

    running_waiter = utils.ec2_client.get_waiter("instance_status_ok")
    running_waiter.wait(InstanceIds=[i.instance_id for i in vm_instances.values()])
    
    for i in vm_instances.values():
        i.load()
        print(f"Instance {i.instance_id} created !")
        print(f"\tAvailable at {i.public_dns_name} ({i.public_ip_address}).")