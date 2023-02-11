#!/usr/bin/env python3.9

import os
import subprocess
file = '/home/malav/mb_pandas'

#subprocess.run(["cd",file]), check=True, stdout=subprocess.PIPE).stdout
os.system('cd ' + file)

if os.path.exists(file+'/dist'):
    os.system('rm -rf '+file+'/dist')
    os.system('rm -rf '+file+'/build')
#subprocess.run(["ls"]),check=True, stdout=subprocess.PIPE).stdout
os.system("ls")
subprocess.run(["git", "pull"], check=True, stdout=subprocess.PIPE).stdout
#os.system('git pull')
os.system('python3.9 -m setup bdist_wheel')
os.system('python3.9 -m pip install '+file + '/dist/' +os.listdir(file +'/dist')[-1])
