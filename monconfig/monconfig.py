#!/usr/bin/env python

from mako.template import Template
import yaml

yamlfile = open('testconfig.yaml','r')

hostinfo = yaml.load(yamlfile)

def config_smokeping(hostinfo):

  targettemplate = Template(filename='templates/smokeping/Targets.tmpl')

  smokeconfig = targettemplate.render(hostinfo=hostinfo)
  print smokeconfig

  smokefile = open('/etc/smokeping/config.d/Targets','w');
  smokefile.write(smokeconfig)
  smokefile.close

config_smokeping(hostinfo)
