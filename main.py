#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import getpass
import traceback
import ConfigParser
import rtmp_protocol
import gdata.spreadsheet.service

from sets import Set
from datetime import datetime


password = None


class SO(rtmp_protocol.FlashSharedObject):
    def __init__(self, config):
        rtmp_protocol.FlashSharedObject.__init__(self, 'live_shared')
        
        email = config.get('Core', 'GoogleLogin')
        
        global password
        if password is None:
            password = getpass.getpass("Enter password for %s: " % email)
        
        self.spreadsheet_key = config.get('Core', 'SpreadsheetKey')
        
        self.spr_client = gdata.spreadsheet.service.SpreadsheetsService()
        self.spr_client.email = email
        self.spr_client.password = password
        self.spr_client.source = 'Microwave Cutter'
        self.spr_client.ProgrammaticLogin()
        
        self.rj_list = Set()

    def on_message(self, key):
        if key[0] == u'SetLiveStatus':
            self.update_broadcasters(key[1])
        elif key[0] == u'SetLCount':
            self.update_viewers(key[1])
        elif key[0] == u'SetStreamNick':
            self.update_stream_nick(int(key[1]), key[2])
        else:
            print('SO message ' + key[0])

    def update_broadcasters(self, broadcasters):
        print('broadcasters ' + str(broadcasters).decode('unicode-escape'))
        
        dline = {}
        new_rj_list = Set()
        
        for bc in broadcasters:
            if hasattr(bc, 'desc'):
                new_rj_list.add(bc['desc'])
        
        now = datetime.utcnow()
        cur_date = now.strftime("%m/%d/%Y")
        cur_time = now.strftime("%H:%M:%S")
        
        if new_rj_list == self.rj_list:
            print('No update in rotation list')
        else:
            rj_entry = {'rj' + str(i) : rj for i, rj in enumerate(new_rj_list)}
            rj_entry['date'] = cur_date
            rj_entry['time'] = cur_time
            print(rj_entry)
            entry = self.spr_client.InsertRow(rj_entry, self.spreadsheet_key, 4)
        
        self.rj_list = new_rj_list
        
        for idx, bc in enumerate(broadcasters):
            whitelist = ['author', 'volume', 'mode', 'mediaName', 'id', 'desc']
            line = {i.lower()+str(idx):unicode(bc[i]) for i in bc if i in whitelist}
            dline = dict(dline.items() + line.items())
        
        dline['date'] = cur_date
        dline['time'] = cur_time
        print(str(dline).decode('unicode-escape'))
        entry = self.spr_client.InsertRow(dline, self.spreadsheet_key, 1)
    
    def update_viewers(self, viewers):
        print('viewers', viewers)
        now = datetime.utcnow()
        entry = self.spr_client.InsertRow({
            'datetime': now.strftime("%m/%d/%Y %H:%M:%S"),
            'viewers': str(viewers),
        }, self.spreadsheet_key, 2)
    
    def update_stream_nick(self, index, nick):
        print('stream_nick', index, nick)
        
        now = datetime.utcnow()
        entry = self.spr_client.InsertRow({
            'datetime': now.strftime("%m/%d/%Y %H:%M:%S"),
            'index': str(index),
            'nick': nick,
        }, self.spreadsheet_key, 3)


def main():
    config = ConfigParser.ConfigParser()
    config.read(['config.ini'])
    
    channel_id = config.getint('Core', 'Channel')
    unique_id = config.get('Core', 'UniqueId')
    
    client = rtmp_protocol.RtmpClient(channel_id, unique_id)
    client.connect()
    
    live_shared = SO(config)
    client.shared_object_use(live_shared)
    
    client.call('Initialize', [None, 'normal', '', '1fde', '', '', '', 'Username'])

    client.handle_messages()


if __name__ == '__main__':
    while True:
        try:
            main()
        except:
            print traceback.format_exc()
            print('Caught exception, restarting in 5 seconds')
            time.sleep(5)
            pass
        else:
            break
