#!/usr/bin/python

"""Oozie proxy

    Since Oozie HTTP enables the Kerberos authentication
"""

import asynchat
import asyncore
import socket
import string
import re

class proxy_server (asyncore.dispatcher):

    def __init__ (self, host, port, listen_port):
        asyncore.dispatcher.__init__ (self)
        self.create_socket (socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.there = (host, port)
        here = ('', listen_port)
        self.bind (here)
        self.listen (5)
    
    def handle_accept (self):        
        proxy_receiver (self, self.accept())

class proxy_receiver (asynchat.async_chat):
    HTTP_LF = '\r\n' # constant, DONT CHANGE IT!!!!
    channel_counter = 0
    
    def connect_to_oozie(self, curl_cmd):       
        for idx in range(1, 10):
            import commands
            output = commands.getoutput(curl_cmd)
            if 'Mechanism level: Request is a replay' in output:
                from time import sleep
                sleep(0.05)
                continue
            return output
            
    def send_by_segment(self, data):
        separate_idx = data.find(self.HTTP_LF+self.HTTP_LF)
        header = data[:separate_idx]
        body = data[separate_idx+4:]
        self.push(header+self.HTTP_LF+self.HTTP_LF)
        
        n = 0x1000
        lines = [body[i:i+n] for i in range(0, len(body), n)] # split into string list with each size as 0x1000
        for line in lines:
            self.push('%x' % len(line) + self.HTTP_LF)
            self.push(line + self.HTTP_LF)
        self.push('0' + self.HTTP_LF)
        self.push(self.HTTP_LF)

    def __init__ (self, server, (conn, addr)):
        asynchat.async_chat.__init__ (self, conn)
        self.set_terminator ('\n')
        self.server = server
        self.id = self.channel_counter
        self.channel_counter = self.channel_counter + 1
        self.buffer = ''
    
    def collect_incoming_data (self, data):
        self.buffer = self.buffer + data
        
    def found_terminator (self):
        data = re.sub( r'\:8080', '', self.buffer )
        data = re.sub( r'localhost', self.server.there[0], data )
        self.buffer = ''
        m = re.search('GET (.*) HTTP.*', data)
        if m:
            curl_cmd = 'curl -s -D - --negotiate -u : ' + '"' + "http://%s:%s" % (self.server.there[0], self.server.there[1]) + m.group(1) + '"';
            output = self.connect_to_oozie(curl_cmd)           
            if '401 Unauthorized' in output:
                output = output[output.find(self.HTTP_LF+self.HTTP_LF)+4:] # remove first http reponse header because of SPNEGO
            if 'Content-Length' not in output:
                self.send_by_segment(output)
            else:
                self.push(output + '\n')
                
    def handle_close (self):        
        if len( self.buffer ):
            self.found_terminator()
        
        self.close()

def show_usage():
    print'''Usage:
    python oozie_proxy.py <oozie_host:port> [listen_port]
    
    oozie_proxy.py will listen on listen_port, redirect all HTTP traffic to oozie_host:port
    If no specify listen_port, it will listen on port+8000'''

if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2 or not re.search('.*:.*', sys.argv[1]):
        show_usage()
        sys.exit(0)

    host = ''
    port = ''
    listen_port = 0

    host = sys.argv[1].split(':')[0]
    port = sys.argv[1].split(':')[1]
    if len(sys.argv) == 3:
        listen_port = int(sys.argv[2])
    else:
        listen_port = int(port) + 8000
    
    HOST = host
    PORT = port    
    ps = proxy_server (host, int(port), listen_port)
    asyncore.loop()
