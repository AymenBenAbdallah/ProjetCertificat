import utils

core_site_conf = """
<configuration> 
    <property> 
        <name>hadoop.tmp.dir</name> 
        <value>/tmp/hadoop</value> 
        <description>A base for other temporary directories.</description> 
    </property> 
    <property> 
        <name>fs.defaultFS</name> 
        <value>hdfs://master:54310</value> 
    </property> 
</configuration>
"""

hdfs_site_conf = """
<configuration> 
    <property> 
        <name>dfs.replication</name> 
        <value>1</value> 
    </property> 
    <property> 
            <name>dfs.block.size</name> 
            <value>67108864</value> 
    </property> 
</configuration>
"""

def setup_master_nfs(master):
    master.sudo("apt-get -y install nfs-kernel-server nfs-common", hide='both')
    master.sudo("mkdir -p /mnt/nfs")
    master.run("echo '/mnt/nfs/ *(rw,sync,no_root_squash)' | sudo tee -a /etc/exports", hide='stdout')
    master.sudo("systemctl restart nfs-kernel-server")

def setup_slave_nfs(slave_client, master_priv_ip):
    slave_client.sudo("apt-get -y install nfs-common", hide='both')
    slave_client.sudo("mkdir -p /mnt/nfs")
    slave_client.sudo(f"mount -t nfs {master_priv_ip}:/mnt/nfs /mnt/nfs")

def get_spark_and_dependencies(master):
    master.sudo("curl -fsSLo /mnt/nfs/jdk.tar.gz http://sd-127206.dedibox.fr/hagimont/software/jdk-8u221-linux-x64.tar.gz")
    master.sudo("curl -fsSLo /mnt/nfs/hadoop.tar.gz http://sd-127206.dedibox.fr/hagimont/software/hadoop-2.7.1.tar.gz")
    master.sudo("curl -fsSLo /mnt/nfs/spark.tar.gz http://sd-127206.dedibox.fr/hagimont/software/spark-2.4.3-bin-hadoop2.7.tgz")
    master.sudo("curl -fsSLo /mnt/nfs/ressources.tar.gz http://sd-127206.dedibox.fr/hagimont/resources-N7/bigdata/sujet-tp-scale.tgz")

def setup_common(ssh_client):
    ssh_client.run("for i in /mnt/nfs/*.tar.gz; do tar -C ~/ -xzf $i; done")

    ssh_client.run("echo 'JAVA_HOME=/home/ubuntu/jdk1.8.0_221\n" +
                        "SPARK_HOME=/home/ubuntu/spark-2.4.3-bin-hadoop2.7\n" +
                        "HADOOP_HOME=/home/ubuntu/hadoop-2.7.1' | sudo tee /etc/environment", hide='both')
    ssh_client.run("echo 'PATH=/home/ubuntu/hadoop-2.7.1/bin:/home/ubuntu/hadoop-2.7.1/sbin:" +
                        "/home/ubuntu/spark-2.4.3-bin-hadoop2.7/bin:/home/ubuntu/spark-2.4.3-bin-hadoop2.7/sbin:" +
                        "/home/ubuntu/jdk1.8.0_221/bin:'$PATH " +
                        "| sudo tee -a /etc/environment", hide='both')
                        
    ssh_client.run("sed -i 's+${JAVA_HOME}+~/jdk1.8.0_221+g' ~/hadoop-2.7.1/etc/hadoop/hadoop-env.sh")
    ssh_client.run("cp ~/spark-2.4.3-bin-hadoop2.7/conf/spark-env.sh.template ~/spark-2.4.3-bin-hadoop2.7/conf/spark-env.sh")
    ssh_client.run("echo 'export SPARK_MASTER_HOST=master\nexport JAVA_HOME=~/jdk1.8.0_221' " +
                    "| sudo tee -a ~/spark-2.4.3-bin-hadoop2.7/conf/spark-env.sh", hide='both')
    ssh_client.run("sed -in '/<\/*configuration>/d' ~/hadoop-2.7.1/etc/hadoop/core-site.xml")
    ssh_client.run(f"echo '{core_site_conf}' | sudo tee -a ~/hadoop-2.7.1/etc/hadoop/core-site.xml", hide='both')

def setup_hadoop(master, nb_slaves):
    master.run("sed -in '/<\/*configuration>/d' ~/hadoop-2.7.1/etc/hadoop/hdfs-site.xml")
    master.run(f"echo '{hdfs_site_conf}' | sudo tee -a ~/hadoop-2.7.1/etc/hadoop/hdfs-site.xml", hide='both')
    master.run("sed -in '/localhost/d' ~/hadoop-2.7.1/etc/hadoop/slaves")

    for i in range(nb_slaves):
        master.run(f"echo 'slave{i}' | sudo tee -a ~/hadoop-2.7.1/etc/hadoop/slaves", hide='both')

def setup_spark(master, nb_slaves):
    master.run("cp ~/spark-2.4.3-bin-hadoop2.7/conf/slaves.template ~/spark-2.4.3-bin-hadoop2.7/conf/slaves")
    master.run("sed -in '/localhost/d' ~/spark-2.4.3-bin-hadoop2.7/conf/slaves")

    # Recreate a new start.sh to scale with the number of slaves
    master.run("echo 'rm -rf /tmp/hadoop*' | sudo tee ~/sujet-tp-scale/start.sh", hide='both')

    for i in range(nb_slaves):
        master.run(f"echo 'slave{i}' | sudo tee -a ~/spark-2.4.3-bin-hadoop2.7/conf/slaves", hide='both')
        master.run(f"echo 'ssh slave{i} rm -rf /tmp/hadoop*' | sudo tee -a ~/sujet-tp-scale/start.sh", hide='both')
    
    master.run("echo 'hdfs namenode -format \nstart-dfs.sh \nstart-master.sh \nstart-slaves.sh' " +
                        "| sudo tee -a ~/sujet-tp-scale/start.sh", hide='both')

def deploy_spark(vm_instances):
    master = utils.get_master(vm_instances)
    slaves = utils.get_slaves(vm_instances)
    nb_slaves = len(slaves)

    master.ssh = utils.connect_ssh(master.public_ip_address)

    setup_master_nfs(master.ssh)

    get_spark_and_dependencies(master.ssh)

    setup_common(master.ssh)

    setup_hadoop(master.ssh, nb_slaves)

    setup_spark(master.ssh, nb_slaves)
    
    for slave in slaves:
        print("\t", end='', flush=True)
        slave.ssh = utils.connect_ssh(slave.public_ip_address)
        setup_slave_nfs(slave.ssh, master.private_ip_address)
        setup_common(slave.ssh)
        slave.ssh.close()

    master.ssh.close()
