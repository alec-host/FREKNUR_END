#!/usr/bin/python2

import os
import re
import sys
import json
import MySQLdb
import logging
import signal
import Queue

from conn.model import _read_loan_request_queue_sys,_queue_loan_sys
from conn.db_helper import create_connection,close_connection,NoResultException
from conn.configs.freknur_settings import loan_params

log = logging.getLogger()

def handleLoanRequest():
	db = create_connection()
	try:
		while True:
			"""
			-.get loan request list.
			"""
			items = json.loads(_read_loan_request_queue_sys(db))	
			"""
			-.loop through each in the list
			"""
			for item in items:
				"""
				-.check for loan allowed.
				"""
				if(str(item['amount']) < loan_params['min_loan']):
					"""
					-.json output.
					"""			
					print('{"ERROR":"1","RESULT":"FAIL","MESSAGE":"Allowed minimun loan is KES."'+str(loan_params['min_loan'])+'}')
				elif(loan_params['max_loan'] > str(item['amount'])):
					"""
					-.json output.
					"""				
					print('{"ERROR":"1","RESULT":"FAIL","MESSAGE":"Allowed maximum loan is KES."'+str(loan_params['max_loan'])+'}')		
				else:
					"""
					-.queue loan & mark as processed.
					"""
					_queue_loan_sys(item['reference_no'],item['msisdn'],str(item['amount']),item['approved_by'],db)
					
	except MySQLdb.Error, e:
		log.error(e)
	except Exception, e:
		log.error(e)
	finally:
		try:
			if(not db):
				exit(0)
			else:
				"""
				close MySQL connection.
				"""			
				close_connection(db)
		except MySQLdb.Error, e:
			log.error(e)
			
"""
-.main method.
"""	
if __name__ == '__main__':
	try:
		handleLoanRequest()
	except KeyboardInterrupt:
		exit();