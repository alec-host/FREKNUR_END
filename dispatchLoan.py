#!/usr/bin/python

import os
import re
import sys
import json
import MySQLdb
import logging
import signal
import Queue

from conn.model import _read_loan_payout_queue_sys,_dispatch_loan_sys,_record_loan_transaction_sys,\
                       _record_loan_fee_sys
from conn.configs.freknur_settings import loan_params,accounting_params
from conn.db_helper import create_connection,close_connection,NoResultException

log = logging.getLogger()

def dispatchLoan():
	db = create_connection()
	try:
		
		while True:
			"""
			-.get loan request list.
			"""
			items = json.loads(_read_loan_payout_queue_sys(db))	
			"""
			-.loop through each in the list
			"""
			for item in items:		
				"""
				-.calc loan handling fees.
				"""
				handle_fee = int(item['amount']) * float(loan_params['loan_fee'])
				"""
				-.calc amount loaned.
				"""
				loan_amount = int(item['amount']) - float(handle_fee)
				"""
				-.calc interest amount.
				"""
				interest_amount = (float(loan_params['interest']) * int(item['amount']))
				"""
				-.calc repayment amount = (principle+interest).
				"""
				repayment_amount = (int(item['amount']) + interest_amount)
				"""
				-.log trx affecting utility a/c.
				"""
				_record_loan_transaction_sys(str(item['amount']),item['msisdn'],item['reference_no'],str(eval(accounting_params['accounts'])[1]),str(accounting_params['debit']),db)
				"""
				-.log loan fee.
				"""
				_record_loan_fee_sys(item['msisdn'],item['reference_no'],str(handle_fee),str(eval(accounting_params['accounts'])[3]),db)				
				"""
				-.update wallet bal & log the transaction.
				"""
				_dispatch_loan_sys(item['reference_no'],item['msisdn'],str(item['amount']),str(loan_amount),str(repayment_amount),str(interest_amount),loan_params['duration'],loan_params['notify_1'],db)
				
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
		dispatchLoan()
	except KeyboardInterrupt:
		exit();