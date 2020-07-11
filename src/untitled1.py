#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 10 16:40:10 2020

@author: pyahia
"""



    # def auth(self, data): #get username. Ideally, this might be a able to be a bit cleaner TODO: Valdiate JDBC VS ODBC
    #     """ Grab user_name during initial handshake"""
    #     if b"user" in data:
    #         self._user = re.search("user\x00(.+)\x00",data.decode('latin1')).group(1).split("\x00")[0]
    #         self.build_grants()
        
    # def validate_cache_access_permissions(self, key):
    #     """ Check objects in cache, validate permissions are granted on all objects"""
    #     targets_requested = query_cache.cache_table[key]
    #     access_granted = True
    #     for target in targets_requested:
    #         if target not in self._read_grants:
    #             access_granted = False
    #             break
    #     return access_granted
                
    # def build_grants(self):
    #     """ Get a list of object-grants on session-user"""
    #     roles = list(self.priv_mgr.user_role_df.loc[self.priv_mgr.user_role_df.user_name == self._user]["role"].values)
    #     grants = list(self.priv_mgr.grant_user_df.loc[self.priv_mgr.grant_user_df.grantee == self._user].obj.values)
    #     for role in roles:
    #         grants += list(self.priv_mgr.grant_role_df.loc[self.priv_mgr.grant_role_df.grantee == role].obj.values)
    #     self._read_grants = grants