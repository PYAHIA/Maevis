#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 10 16:23:51 2020

@author: pyahia
"""


    def write(self, data):
        print("w\n")
        print(data)
        """ Handle Messages from Server to Client"""
        msg = self.sp_data[0]

        if msg:
            print(msg.cachable)
            if msg.proto_ord == _REQUEST_EXT_ORD and msg.cachable == False:
                msg.set_extended_protocol_flag()
                print("set true")
                print(msg.cachable,"")
            else:
                self.sp_data[1].append(data)
                if msg.proto ==_REQUEST_EXT_ORD and msg.cachable == True:
                    if data[-2:] in (_END_JDBC_PATTERN): 
                      print("WRITING TO CACHE")
                      print(len(str(self.sp_data[1]).encode()) )
                      if len(str(self.sp_data[1]).encode()) < _MAX_RESULT_SIZE:
                          query_cache.write_to_cache(msg, self.sp_data[1])
                elif msg.proto == _REQUEST_ORD:
                    if data[-2:] in (_END_PATTERN, _END_JDBC_PATTERN): 
                        print("WRITING TO CACHE")
                        print(len(str(self.sp_data[1]).encode()) )
                        if len(str(self.sp_data[1]).encode()) < _MAX_RESULT_SIZE:
                            query_cache.write_to_cache(msg, self.sp_data[1])