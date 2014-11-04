#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import getpass
import rtmp_protocol
import gdata.spreadsheet.service

from datetime import datetime


class SO(rtmp_protocol.FlashSharedObject):
    def __init__(self):
        rtmp_protocol.FlashSharedObject.__init__(self, 'live_shared')
        
        email = 'mikanasai@gmail.com'
        password = getpass.getpass("Enter password for %s:" % email)
        
        self.spreadsheet_key = '1EqpAEvUIivHZGyDysl16ZSL4jaUZgNd9VTN8Nr98TVQ'
        
        self.spr_client = gdata.spreadsheet.service.SpreadsheetsService()
        self.spr_client.email = email
        self.spr_client.password = password
        self.spr_client.source = 'Microwave Cutter'
        self.spr_client.ProgrammaticLogin()

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
        for idx, bc in enumerate(broadcasters):
            whitelist = ['author', 'volume', 'mode', 'mediaName', 'id', 'desc']
            line = {i.lower()+str(idx):unicode(bc[i]) for i in bc if i in whitelist}
            dline = dict(dline.items() + line.items())
        
        now = datetime.utcnow()
        dline['date'] = now.strftime("%d/%m/%Y")
        dline['time'] = now.strftime("%H:%M:%S")
        print(str(dline).decode('unicode-escape'))
        entry = self.spr_client.InsertRow(dline, self.spreadsheet_key, 1)
    
    def update_viewers(self, viewers):
        print('viewers', viewers)
        now = datetime.utcnow()
        entry = self.spr_client.InsertRow({
            'datetime': now.strftime("%d/%m/%Y %H:%M:%S"),
            'viewers': str(viewers),
        }, self.spreadsheet_key, 2)
    
    def update_stream_nick(self, index, nick):
        print('stream_nick', index, nick)
        
        now = datetime.utcnow()
        entry = self.spr_client.InsertRow({
            'datetime': now.strftime("%d/%m/%Y %H:%M:%S"),
            'index': str(index),
            'nick': nick,
        }, self.spreadsheet_key, 3)
        

def main():
    channel_id = 616498
    # channel_id = 643685
    client = rtmp_protocol.RtmpClient(channel_id)
    client.connect()
    
    live_shared = SO()
    client.shared_object_use(live_shared)
    
    client.call('Initialize', 
        [None, 'normal', 
        '',
        '1fde', '', '', '', 'Username'])

    client.handle_messages()

if __name__ == '__main__':
    while True:
        try:
            main()
        except:
            print('Caught exception, restarting in 5 seconds')
            time.sleep(5)
            pass
        else:
            break
