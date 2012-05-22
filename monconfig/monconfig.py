#!/usr/bin/env python

import os
from time import sleep
from subprocess import call
from mako.template import Template
import yaml

yamlfile = open('testconfig.yaml','r')

hostinfo = yaml.load(yamlfile)

def config_smokeping(hostinfo):

  targettemplate = Template(filename='templates/smokeping/Targets.tmpl')

  smokeconfig = targettemplate.render(hostinfo=hostinfo)
#  print smokeconfig

  smokefile = open('/etc/smokeping/config.d/Targets','w');
  smokefile.write(smokeconfig)
  smokefile.close

def launch_mtr(hostinfo):
  for location in hostinfo:
    for host in hostinfo[location]:
      ip = hostinfo[location][host]['ip']
      screencmd = "screen -d -m -S " + host + " mtr -n --curses " + ip
      os.system(screencmd)
      print screencmd


launch_mtr(hostinfo)
config_smokeping(hostinfo)
call(['/usr/sbin/service','smokeping','restart'])
