from bitforgehublog import MultiSinkSink,BitForgeHubLogParser
from win32com.client import Dispatch
from collections import defaultdict
import argparse
import datetime

class InstrumentToDbSink(object):
  def __init__(self, connection_string, insertion_interval=0):
    self.instruments = defaultdict(dict)
    self.handles = {}
    self.cn = Dispatch('ADODB.Connection')
    self.insertion_queue = []
    self.insertion_interval = insertion_interval
    self.last_insertion = datetime.datetime.now()
    print 'connecting to ', connection_string
    self.cn.Open(connection_string)

  def on_log_entry(self, log_entry):
    if log_entry.command == 'create':
      items = log_entry.key.split('/')
      if items[-1] == 'properties':
        self.handles[log_entry.handle] = items[-2]

    elif log_entry.command == 'set':
      symbol = self.handles.get(log_entry.handle)

      if symbol is None:
        return
      
      self.instruments[symbol][log_entry.key] = log_entry.value
      self.__send_symbol_to_db(symbol)

def main():
  argparser = argparse.ArgumentParser('Replay BitForgeHub logs')
  argparser.add_argument('hub')
  argparser.add_argument('file_path')
  argparser.add_argument('--speed', default=0, type=int)
  argparser.add_argument('--delay', default=0, type=int)
  argparser.add_argument('--follow', action='store_true')
  argparser.add_argument('--pause', action='store_true', help='Pauses the server while loading. **This will disconnect all client during load time**')

  params = argparser.parse_args()

  print 'Loading file "%s" to BitForgeHub @ %s, %d msgs/sec, %d seconds delay %s, %s' % \
    (params.file_path, params.hub, params.speed,
     params.delay, '(follow file)' if params.follow else '', ' (pause server while loading)' if params.pause else '')

  multisink = MultiSinkSink()

  multisink.sinks.append(InstrumentToDbSink('Provider=OraOLEDB.Oracle.1;Password=123mudar;Persist Security Info=True;User ID=system;Data Source=localhost'))

  parser = BitForgeHubLogParser(multisink)

  if params.pause:
    hub_sink.hub.pause()

  try:
    parser.replay(
      params.file_path,
      params.speed,
      params.delay,
      params.follow)
    
  finally:
    if params.pause:
      hub.server_resume()
    

if __name__ == '__main__':
  main()

