#!/usr/bin/python

"""
developer skype: alec_host
"""
import os
import sys
import time
import signal
import json
import ast
import eventlet
import logging
import MySQLdb
import MySQLdb.cursors

from collections import OrderedDict
from datetime import datetime
from configs.freknur_settings import logger,mysql_params
from db_conn import DB,NoResultException,NoServiceIDException

eventlet.monkey_patch()

db = DB()

"""
-=================================================
-.record new loan request.
-=================================================
"""	
def _record_loan_request_db(msisdn,amount,conn):
	jsString = None
	try:
		sql = """CALL """+mysql_params['db']+""".`sProcLoanRequest`(%s,%s)"""
		params = (msisdn,amount,)
		output = db.retrieve_all_data_params(conn,sql,params)	
		
		for data in output:
			jsString = json.loads(data.get('_JSON'))
			
		conn.commit()
	except Exception, e:
		logger.error(e)
		raise	
	
	return jsString
	
"""
-=================================================
-.record new loan request.
-=================================================
"""
	
def _registration_db(msisdn,passwd,conn):
	jsString = None
	try:                                                                                                                                                                                                                                                                       
		sql = """CALL """+mysql_params['db']+""".`sProcActivateWallet`(%s,%s)"""
		params = (msisdn,passwd,)
		output = db.retrieve_all_data_params(conn,sql,params)	
		
		for data in output:
			jsString = json.loads(data.get('_JSON'))
			
		conn.commit()
	except Exception, e:
		logger.error(e)
		raise	
	
	return jsString
	
"""
-=================================================
-.queue loan.
-=================================================
"""	
def _queue_loan_db(reference_no,msisdn,amount,approved_by,conn):
	jsString = None
	try:
		sql = """CALL """+mysql_params['db']+""".`sProcQueueLoan`(%s,%s,%s,%s)"""
		params = (reference_no,msisdn,amount,approved_by,)
		output = db.retrieve_all_data_params(conn,sql,params)	
		
		for data in output:
			jsString = json.loads(data.get('_JSON'))
			
		conn.commit()
	except Exception, e:
		logger.error(e)
		raise	
	
	return jsString
	
"""
-=================================================
-.queue loan.
-=================================================
"""	
def _dispatch_loan_db(reference_no,msisdn,amount,loan_amount,repayment_amount,interest_amount,loan_duration,notify_1,conn):
	jsString = None
	try:
		sql = """CALL """+mysql_params['db']+""".`sProcLoanDispatch`(%s,%s,%s,%s,%s,%s,%s,%s)"""
		params = (reference_no,msisdn,amount,loan_amount,repayment_amount,interest_amount,loan_duration,notify_1)
		output = db.retrieve_all_data_params(conn,sql,params)	
		
		for data in output:
			jsString = json.loads(data.get('_JSON'))
			
		conn.commit()
	except Exception, e:
		logger.error(e)
		raise	
	
	return jsString
	
"""
-=================================================
-.log book keeping transaction.
-=================================================
"""	
def _record_loan_transaction_db(amount,msisdn,reference_no,account_name,transaction_type,conn):
	jsString = None
	try:
		sql = """CALL """+mysql_params['db']+""".`sProcLogTransactions`(%s,%s,%s,%s,%s)"""
		params = (amount,msisdn,reference_no,account_name,transaction_type)
		output = db.retrieve_all_data_params(conn,sql,params)	
		
		for data in output:
			jsString = json.loads(data.get('_JSON'))
			
		conn.commit()
	except Exception, e:
		logger.error(e)
		raise	
	
	return jsString
	
"""
-=================================================
-.log loan fee.
-=================================================
"""	
def _record_loan_fee_db(msisdn,reference_no,loan_fee,account_name,conn):
	jsString = None
	try:
		sql = """CALL """+mysql_params['db']+""".`sProcLogLoanFee`(%s,%s,%s,%s)"""
		params = (msisdn,reference_no,loan_fee,account_name)
		output = db.retrieve_all_data_params(conn,sql,params)	
		
		for data in output:
			jsString = json.loads(data.get('_JSON'))
			
		conn.commit()
	except Exception, e:
		logger.error(e)
		raise	
	
	return jsString

"""
-=================================================
-.queued loan payout list.
-=================================================
"""
def _loan_payout_list_db(flag,lower_min,lower_max,conn):
	jsString = None
	try:
		sql  =   """
				 SELECT 
				`reference_no`,`msisdn`,`amount`,`status`,`approved_by`,CONCAT("'",`date_created`,"'") AS date_created,CONCAT("'",`date_modified`,"'") AS date_modified 
				 FROM 
				`tbl_loan_payout`
				 WHERE 
				`is_processed` = %s 
                 LIMIT %i, %i 
				 """ % (flag,int(lower_min),int(lower_max))
				 
		params = ()
		
		recordset = db.retrieve_all_data_params(conn,sql,params)
		
		jsonArray = ast.literal_eval(json.dumps(recordset))
		jsonArraySize = len(jsonArray)
		
		jsString = '{"Result":"OK","Records":'+str(json.dumps(recordset))+',"TotalRecordCount":'+str(jsonArraySize)+'}'
				
	except Exception, e:
		logger.error(e)
		raise	
	
	return jsString
	
"""
-=================================================
-.get loan request list.
-=================================================
"""
def _loan_request_list_db(flag,lower_min,lower_max,conn):
	jsString = None
	try:
		sql  =   """
				 SELECT 
				`reference_no`,`msisdn`,`amount`,`requested_by`,IFNULL(`task_flag`,0) AS task_flag,CONCAT("'",`date_created`,"'") AS date_created,CONCAT("'",`date_modified`,"'") AS date_modified
				 FROM 
				`tbl_loan_request`
				 WHERE 
				`is_processed` = %s 
                 LIMIT %i, %i 
				 """ % (flag,int(lower_min),int(lower_max))
				 
		params = ()
		
		recordset = db.retrieve_all_data_params(conn,sql,params)
		
		jsonArray = ast.literal_eval(json.dumps(recordset))
		jsonArraySize = len(jsonArray)
		
		jsString = '{"Result":"OK","Records":'+str(json.dumps(recordset))+',"TotalRecordCount":'+str(jsonArraySize)+'}'
				
	except Exception, e:
		logger.error(e)
		raise	
	
	return jsString

"""
-=================================================
-.loan approval operation.
-=================================================
"""	
def _loan_approval_operation_db(reference_no,msisdn,approved_by,conn):
	processZ = 0
	taskFlag = 2
	idateNow = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	jsString = None
	try:
		sql = """
			  UPDATE 
			 `tbl_loan_request` 
			  SET  
			 `task_flag` = %s, 
			 `approved_by` = '%s',
			 `date_modified` = '%s'
			  WHERE 
			 `reference_no` = '%s' AND `msisdn` = %s AND `is_processed` = %s
			  """ % (taskFlag,approved_by,idateNow,reference_no,msisdn,processZ)
			  
		params = ()
		#db.retrieve_all_data_params(conn, qry, params)
		db.execute_query(conn, sql, params)
		
		jsString = {"Result":"OK"}
		
		conn.commit()
		
	except Exception, e:
		logger.error(e)
		print(e)
		raise

	return jsString
	
"""
-=================================================
-.loan approval operation.
-=================================================	
"""	
def _get_loan_request_db(conn,limit=1000):
	processZ = 0
	taskFlag = 2
	jsString = None
	try:
		sql = """
		      SELECT 
		     `id`,`msisdn`,`reference_no`,`amount`,`approved_by`,CONCAT("'",`date_created`,"'") AS date_created
		      FROM 
			 `tbl_loan_request`
		      WHERE
		     `is_processed` = %s AND `task_flag` = %s
		      LIMIT %s		  
		      """
			  
		params = (processZ,taskFlag,limit)
	
		recordset = db.retrieve_all_data_params(conn,sql,params)
		
		jsString = json.dumps(recordset)
	
	except Exception,e:
		logger.error(e)
		raise
	
	return jsString
	
"""
-=================================================
-.loan approval operation.
-=================================================	
"""	
def _get_loan_payout_db(conn,limit=1000):
	process = 0
	status  = 0
	jsString = None
	try:
		sql = """
		      SELECT 
		     `id`,`reference_no`,`msisdn`,`amount`
		      FROM 
			 `tbl_loan_payout`
		      WHERE
		     `is_processed` = %s AND `status` = %s
		      LIMIT %s		  
		      """
			  
		params = (process,status,limit)
	
		recordset = db.retrieve_all_data_params(conn,sql,params)
		
		jsString = json.dumps(recordset)
	
	except Exception,e:
		logger.error(e)
		raise
	
	return jsString	
		
"""
-=================================================
-.mark load as queued.
-=================================================	
"""	
def _mark_loan_request_processed_db(_id,msisdn,ref_no,conn):
	processZ = 0
	processO = 1
	taskFlag = 2
	jsString = None
	idateNow = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	try:
		sql = """
			  UPDATE
			 `tbl_loan_request`
			  SET
			 `is_processed` = %s,
			 `date_modified` = '%s',
			 `task_flag` = %s
		      WHERE
		     `is_processed` = %s AND `id` = %s AND `msisdn` = '%s' AND `reference_no` = '%s'	  
		      """ % (processO,idateNow,taskFlag,processZ,_id,msisdn,ref_no)
		
		params = ()
	
		db.execute_query(conn, sql, params)
		
		jsString = {"Result":"OK"}
		
		conn.commit()
	
	except Exception,e:
		logger.error(e)
		raise
	
	return jsString
	
	
"""
-========================================================================================================================================================================================================

-========================================================================================================================================================================================================
"""
	
		
"""
-=================================================
"""

"""
#-.read inbound sms.
"""
def _read_incoming_sms_db(connection,limit=1000):
	processFlag = 0
	try:
		qry = """
			  SELECT 
			 `ID`,`MSISDN`,`Message`,`ShortCode`
			  FROM
			  """+mysql_params['db']+""".`tbl_incoming_sms` 
			  WHERE 
			 `IsProcessed` = %s
			  ORDER BY 
			 `ID` ASC
			  LIMIT %s
			  """	
		params = (processFlag,limit,)
		in_sms = db.retrieve_all_data_params(connection, qry, params)
		
		return in_sms
	except(Exception, e):
		logger.error(e)
		raise		
"""
#-.get the size the queue.
"""
def _sms_queue_cnt_db(connection, limit=1):
	processFlag = 0
	count = 0
	try:
		qry = """
			  SELECT 
			  COUNT(`ID`) AS cnt
			  FROM
			  """+mysql_params['db']+""".`tbl_incoming_sms` 
			  WHERE 
			 `IsProcessed` = %s
			  LIMIT %s
			  """	
		params = (processFlag,limit)
		rows   = db.retrieve_all_data_params(connection,qry,params)
							
		for row in rows:
			count = row['cnt']
			
		return count
	except(Exception, e):
		logger.error(e)
		raise		
"""
#-.mark message[s] as processed.
"""
def _processed_inbound_sms_db(record_id, connection):
	processFlag = 1
	defaultValue = "'0'"
	try:
		if(record_id != defaultValue):
			qry = """
				  UPDATE 
				  """+mysql_params['db']+""".`tbl_incoming_sms` 
				  SET 
				 `DateModified` = %s, 
				 `IsProcessed` = %s
				  WHERE 
				 `ID` IN ("""+record_id+""") 
				  """
			params = (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),processFlag)
			db.execute_query(connection, qry, params)
			
			connection.commit()
	except(Exception, e):
		logger.error(e)
		print(e)
		raise
"""
#-.create new user.
"""	
def _register_on_sms_db(msisdn,connection):
	jsonString = None
	try:
		sql = """CALL """+mysql_params['db']+""".`sProqClientRegister`(%s)"""
		
		params = (msisdn,)
		output = db.retrieve_all_data_params(connection,sql,params)	
		
		for data in output:
			jsonString = json.loads(data.get('json_result'))

	except(Exception, e):
		logger.error(e)
		raise	
		
	return jsonString['RESULT']	
"""
#-.queue outgoing message.
"""	
def _queue_outgoing_sms_db(destination,source,message,connection):
	jsonString = None
	try:
		sql = """CALL """+mysql_params['db']+""".`sProqHandleOutboundSms`(%s,%s,%s)"""
		
		params = (destination,source,message)
		output = db.retrieve_all_data_params(connection,sql,params)	
		
		for data in output:
			jsonString = json.loads(data.get('json_result'))

	except(Exception, e):
		logger.error(e)
		raise	
		
	return jsonString['RESULT']		
"""
#-.queue outgoing message.
"""	
def _verify_customer_db(msisdn,pin,connection):
	jsonString = None
	try:
		sql = """CALL """+mysql_params['db']+""".`sProqClientAuthentication`(%s,%s)"""
		
		params = (msisdn,pin)
		output = db.retrieve_all_data_params(connection,sql,params)	
		
		for data in output:
			jsonString = json.loads(data.get('json_result'))

	except(Exception, e):
		logger.error(e)
		raise	
		
	return [jsonString['RESULT'],jsonString['INFO']]
"""
#-.queue outgoing message.
"""	
def _withdraw_cash_db(msisdn,amount,connection):
	jsonString = None
	try:
		sql = """CALL """+mysql_params['db']+""".`sProqClientWithdraw`(%s,%s)"""
		
		params = (msisdn,amount)
		output = db.retrieve_all_data_params(connection,sql,params)	
		
		for data in output:
			jsonString = json.loads(data.get('json_result'))

	except(Exception, e):
		logger.error(e)
		raise	
		
	return jsonString['RESULT']
"""
#-.queue outgoing message.
"""	
def _log_wallet_cash_movement_db(msisdn,amount,flag,connection):
	jsonString = None
	try:
		sql = """CALL """+mysql_params['db']+""".`sProqLogWalletMoneyMovement`(%s,%s,%s)"""
		
		params = (msisdn,amount)
		output = db.retrieve_all_data_params(connection,sql,params)	
		
		for data in output:
			jsonString = json.loads(data.get('json_result'))

	except(Exception, e):
		logger.error(e)
		raise	
		
	return jsonString['RESULT']
"""
#-.queue outgoing message.
"""	
def _place_bet_db(msisdn,amount,connection):
	jsonString = None
	try:
		sql = """CALL """+mysql_params['db']+""".`sProqClientPlaceBet`(%s,%s)"""
		
		params = (msisdn,amount)
		output = db.retrieve_all_data_params(connection,sql,params)	
		
		for data in output:
			jsonString = json.loads(data.get('json_result'))

	except(Exception, e):
		logger.error(e)
		raise	
		
	return jsonString['RESULT']	
"""
#-.update bonus.
"""	
def _sync_bonus_db(msisdn,bonus,connection):
	jsonString = None
	try:
		sql = """CALL """+mysql_params['db']+""".`sProqClientUtiliseBonus`(%s,%s)"""
		
		params = (msisdn,amount)
		output = db.retrieve_all_data_params(connection,sql,params)	
		
		for data in output:
			jsonString = json.loads(data.get('json_result'))

	except(Exception, e):
		logger.error(e)
		raise	
		
	return jsonString['RESULT']	
			
"""
#-.db routine connection.
"""
def create_connection():
	try:
		connection = MySQLdb.connect(host=mysql_params['host'],\
					 user=mysql_params['user'], passwd=mysql_params['passwd'],\
					 db=mysql_params['db'], cursorclass=MySQLdb.cursors.DictCursor)
	except(MySQLdb.Error, e):
		logger.error(e)
		raise
	return connection
"""	
#-.db close connection.
"""
def close_connection(connection):
	try:
		connection.close()
	except(MySQLdb.Error, e):
		logger.error(e)
		raise
