import os
import glob
import sys
import json
import signal
import logging
import MySQLdb
import argparse
import datetime
#
DEST_CONFIG = '/etc/phonebook/destination-import.json'
SOURCES_PATH = '/etc/phonebook/sources.d/'
LOG_PATH = '/var/log/phonebook/phonebook-import.log'
#
startTime = None
sourceId = None
importedCount = None
toTransfer = None
errCount = None
sources = None
dest = None
#
def signalHandler(sig, frame):
  logSourceRes()
  logger.critical('interrupted by SIGINT')
  sys.exit(0)

signal.signal(signal.SIGINT, signalHandler)

def test():

  global sourceId
  global importedCount
  global toTransfer
  global errCount
  global startTime
  global sources
  global dest

  logger.info('Start TEST mysql db phonebooks import into phonebook.phonebook')

  # read destination file config
  try:
    with open(DEST_CONFIG, 'r') as configFile:
      dest = json.load(configFile)
      logger.info('check ' + DEST_CONFIG + ': OK')
  except Exception as err:
    logger.error('check ' + DEST_CONFIG + ': FAILED')
    logger.error(str(err))
    sys.exit(1)


  try:
    dbDest = MySQLdb.connect(
      host=dest['host'],
      port=int(dest['port']),
      user=dest['user'],
      passwd=dest['password'],
      db=dest['dbname']
    )
    curDest = dbDest.cursor()
    logger.info('check destination db connection: OK')
  except Exception as err:
    logger.error('check destination db connection: FAILED')
    logger.error(str(err))
    sys.exit(1)

  # cycle all files of sources dir
  filepaths = glob.glob(os.path.join(SOURCES_PATH, '*.json'))
  for f in filepaths:
    try:
      with open(f, 'r') as sourceFile:
        config = json.load(sourceFile)
        logger.info('check source ' + f + ': OK')
    except Exception as err:
      logger.error('check source ' + f + ': FAILED')
      logger.error('reading ' + f)
      logger.error(str(err))

    for sourceId, config in config.items():

      # check if the source is enabled
      if config['enabled'] == False:
        logger.warn('check db source "' + sourceId + '" (' + f + '): DISABLED')
        continue

      # try connection
      try:
        dbSource = MySQLdb.connect(
          host=config['host'],
          port=int(config['port']),
          user=config['user'],
          passwd=config['password'],
          db=config['dbname']
        )
        logger.info('check db source "' + sourceId + '" connection: OK')
      except Exception as err:
        logger.error('check db source "' + sourceId + '" connection: FAILED')
        logger.error(str(err))
        continue

      # clean destination
      try:
        delcount = curDest.execute('SELECT COUNT(*) FROM ' + dest['dbtable'] + ' WHERE source="{}"'.format(sourceId))
        logger.info(str(delcount) + ' entries with "source=' + sourceId + '" will be removed from destination ' + dest['dbname'] + '.' + dest['dbtable'] + ' before importing of source "' + sourceId + '"')
      except Exception as err:
        logger.error('getting number of entries to be remove from destination ' + dest['dbname'] + '.' + dest['dbtable'] + ' before importing of source "' + sourceId + '": FAILED')
        logger.error(str(err))

      # get total number of entries to be copied
      curSource = dbSource.cursor()
      curSource.execute('SELECT COUNT(*) FROM ' + config['dbtable'])
      toTransfer = curSource.fetchone()[0]
      logger.info(str(toTransfer) + ' entries will be copied from db source "' + sourceId + '" into destination ' + dest['dbname'] + '.' + dest['dbtable'])
      curSource.close()
      dbSource.close()

  dbDest.close()
  curDest.close()
  logger.info('End TEST mysql db phonebooks import into phonebook.phonebook')

def start():
  global sourceId
  global importedCount
  global toTransfer
  global errCount
  global startTime
  global sources
  global dest

  logger.warning('START mysql db phonebooks import into phonebook.phonebook')

  # read destination file config
  try:
    with open(DEST_CONFIG, 'r') as configFile:
      dest = json.load(configFile)
  except Exception as err:
    logger.error('reading ' + DEST_CONFIG)
    logger.error(str(err))
    sys.exit(1)

  try:
    dbDest = MySQLdb.connect(
      host=dest['host'],
      port=int(dest['port']),
      user=dest['user'],
      passwd=dest['password'],
      db=dest['dbname']
    )
    curDest = dbDest.cursor()
  except Exception as err:
    logger.error('connecting to the destination db (check ' + DEST_CONFIG + ')')
    logger.error(str(err))
    sys.exit(1)

  # cycle all files of sources dir
  filepaths = glob.glob(os.path.join(SOURCES_PATH, '*.json'))
  for f in filepaths:
    try:
      logger.info('read ' + f)
      with open(f, 'r') as sourceFile:
        config = json.load(sourceFile)
    except Exception as err:
      logger.error('reading ' + f)
      logger.error(str(err))

    for sourceId, config in config.items():

      # check if the source is enabled
      if config['enabled'] == False:
        logger.warn('skip db source "' + sourceId + '"')
        continue

      logger.info('importing source "' + sourceId + '"...')
      startTime = datetime.datetime.now().replace(microsecond=0)
      destCols = config['mapping'].values()
      destCols.append('source')
      sourceCols = config['mapping'].keys()

      # try connection
      try:
        dbSource = MySQLdb.connect(
          host=config['host'],
          port=int(config['port']),
          user=config['user'],
          passwd=config['password'],
          db=config['dbname']
        )
      except Exception as err:
        logger.error('connecting to source db "' + sourceId + '" (check ' + f + ')')
        logger.error(str(err))
        continue

      # clean destination
      try:
        delcount = curDest.execute('DELETE FROM ' + dest['dbtable'] + ' WHERE source="{}"'.format(sourceId))
        logger.info('removed all contacts (#' + str(delcount) + ') of source "' + sourceId + '" from destination ' + dest['dbname'] + '.' + dest['dbtable'])
      except Exception as err:
        logger.error('removing all contacts of source "' + sourceId + '" from destination ' + dest['dbname'] + '.' + dest['dbtable'])
        logger.error(str(err))

      # get total number of entries to be copied
      curSource = dbSource.cursor()
      curSource.execute('SELECT COUNT(*) FROM ' + config['dbtable'])
      toTransfer = curSource.fetchone()[0]

      # start copying
      curSource = dbSource.cursor(MySQLdb.cursors.SSCursor)
      curSource.execute('SELECT ' + ','.join(sourceCols) + ' FROM ' + config['dbtable'])
      row = curSource.fetchone()
      importedCount = 0
      errCount = 0
      while row is not None:
        row = row + (str(sourceId),)
        sql = 'INSERT INTO ' + dest['dbtable'] + ' (' + ','.join(destCols) + ') VALUES {}'.format(row)
        try:
          curDest.execute(sql)
          importedCount += 1
        except Exception as err:
          errCount += 1
          logger.error('error copying contact ' + str(row))
          logger.error(str(err))

        dbDest.commit()
        row = curSource.fetchone()

      curSource.close()
      dbSource.close()
      logSourceRes()
  
  dbDest.close()
  curDest.close()
  logger.warning('END mysql db phonebooks import into phonebook.phonebook')

def logSourceRes():
  global sourceId
  global importedCount
  global toTransfer
  global errCount
  global startTime
  end = datetime.datetime.now().replace(microsecond=0)
  if toTransfer > 0:
    percent = str(importedCount*100/toTransfer)
  else:
    percent = '0'
  logger.warning('source "' + sourceId + '" imported ' + percent + '% (#' + str(importedCount) + ' imported - #' + str(errCount) + ' errors - #' + str(toTransfer) + ' tot - duration ' + str(end-startTime) + ')')

if __name__ == '__main__':
  # parse arguments
  descr = 'MySQL Phonebook importer.\
  You can import more db sources into one sinlge db destination.\
  The sources and destination configuration data has to be declared using json files\
  into the /etc/phonebook/ directory. The import results are written to ' + LOG_PATH + '.'
  parser = argparse.ArgumentParser(description=descr)
  parser.add_argument('-lv', '--log_verbose', action='store_true', help='enable debug log level in ' + LOG_PATH)
  parser.add_argument('-v', '--verbose', action='store_true', help='enable console debug')
  parser.add_argument('-t', '--test', action='store_true', help='test the source and destination configurations making some checks and writing some debug output to the console')
  args = parser.parse_args()
  # logger
  logger = logging.getLogger(__name__)
  logger.setLevel(logging.DEBUG)
  cHandler = logging.StreamHandler()
  fHandler = logging.FileHandler(LOG_PATH)
  cHandler.setLevel(logging.DEBUG if args.verbose == True else logging.NOTSET)
  fHandler.setLevel(logging.DEBUG if args.log_verbose == True else logging.WARNING)
  logFormat = logging.Formatter('%(asctime)s [%(process)s] %(levelname)s: %(message)s', datefmt='%d-%b-%y %H:%M:%S')
  cHandler.setFormatter(logFormat)
  fHandler.setFormatter(logFormat)
  if args.verbose == True or args.test == True:
    logger.addHandler(cHandler)
  logger.addHandler(fHandler)
  if args.test == True:
    test()
  else:
    start()