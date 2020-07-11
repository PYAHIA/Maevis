#!/usr/bin/env python3
# -*- coding: utf-8 -*-
user_sql = """ with /*+ENABLE_WITH_CLAUSE_MATERIALIZATION*/data as (
	SELECT --inherited privs
		principal grantee,
		lower(object_schema || '.' ||object_name) as obj
	FROM INHERITED_PRIVILEGES 
	WHERE privileges_description = 'SELECT'
	UNION ALL 
	SELECT --direct privs
		grantee,
		lower(object_schema || '.' ||object_name)
	FROM grants
	where object_type in ('TABLE','VIEW') 
)
SELECT --user privs
	lower(grantee) as grantee,
	obj
FROM data """

role_sql = """ with data as (
	SELECT --inherited privs
		principal grantee,
		lower(object_schema || '.' ||object_name) as obj
	FROM INHERITED_PRIVILEGES 
	WHERE privileges_description = 'SELECT'
	UNION ALL 
	SELECT --direct privs
		grantee,
		lower(object_schema || '.' ||object_name)
	FROM grants
	where object_type in ('TABLE','VIEW') 
)
SELECT --role based privs
	lower(grantee) as grantee,
	obj
FROM data 
JOIN roles on trim(name) = grantee """


user_role_sql = """
with user_roles as (
	SELECT
		user_name,
		StringTokenizerDelim(all_roles,',') OVER(PARTITION BY user_name) 
	FROM users
)
SELECT
    user_name,
    trim(words) as role
FROM user_roles
"""



import pandas as pd
import vertica_python
import keyring 
from constants import HOST, TARGET_PORT,TARGET_DATABASE


conn_info = {
         'host' : HOST,
         'port': TARGET_PORT,
         'user': 'pyahia',
         'password': keyring.get_password("VSQL_SRVC_ACC", "VQC"),
         'database': TARGET_DATABASE,
         'ssl': False,
         'use_prepared_statements': False
         }

class PrivilegeManager:
    
    def __init__(self):
        self.children = []
        self.access_privileges()
    
    def access_privileges(self):
        with vertica_python.connect(**conn_info) as conn:
            self.grant_user_df = pd.read_sql(user_sql, conn)
            self.grant_role_df = pd.read_sql(role_sql, conn)            
            self.user_role_df = pd.read_sql(user_role_sql, conn)
            
