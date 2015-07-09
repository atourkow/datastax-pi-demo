#!/usr/bin/env python
import sys
import time
import logging
#logging.basicConfig(level=logging.DEBUG)
from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy
from cassandra.policies import TokenAwarePolicy
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.policies import ConstantReconnectionPolicy
from cassandra.policies import DowngradingConsistencyRetryPolicy
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from cassandra import OperationTimedOut

from _config import Config
from random import randint


config = Config()
cluster = Cluster(
  contact_points = config.servers,
)
session = cluster.connect()

keyspace = """
CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'DC1': '3',
  'DC2': '3'
};
"""
keyspace = keyspace % (config.keyspace)
session.execute(keyspace)
session.set_keyspace(config.keyspace)

# Simple table to group times by the hour
table = """
CREATE TABLE IF NOT EXISTS pi_data (
  hour INT,
  time INT,
  temp INT,
  PRIMARY KEY (hour, time)
)
"""
session.execute(table)

# Make a Loop to simulate times
time_delay = 0;
stmt = session.prepare("INSERT INTO pi_data (hour, time, temp) VALUES (?, ?, ?)")
stmt.consistency_level = ConsistencyLevel.LOCAL_ONE
while 1: #just keep going
  time.sleep(0.4)
  rand = randint(1,12)
  temp = randint(1,120)
  time_delay += 1
  # Put data into table

  future = session.execute_async(stmt, (rand, time_delay, temp))
  sys.stdout.write("{} :".format(future._current_host))
  sys.stdout.write(" WROTE : Hour {:<5d} | Time {:<5d} | Temp {:<5d}".format(rand, time_delay, temp))
  try:
    future.result(0.5)
  except OperationTimedOut:
    sys.stdout.write(" TIMED OUT: Reconnecting...")
    cluster.remove_host(future._current_host)
  print " "

