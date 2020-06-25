#!/usr/bin/python2

import os
import sys
import json
import MySQLdb
import logging
import signal
import Queue

from flask import Flask, request, jsonify

from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop

from conn.model import _record_loan_request_api,_get_loan_request_list_api,_get_loan_payout_list_api,_loan_approval_api,_registration_api
from conn.db_helper import create_connection,close_connection,NoResultException
#-pip install tornado.

log = logging.getLogger()

app = Flask(__name__)

"""
-.loan request.
"""
@app.route('/')
@app.route('/recordLoanRequestApi/', methods = ['GET', 'POST'])
def recordLoanRequest():
	db = create_connection()
	try:
		resp = 'Ok'
		if(request.method == 'GET'):
			return "GET method not allowed"
		elif(request.method == 'POST'):	
			content = json.loads(request.data)
			resp = _record_loan_request_api(content,db)
					
		return str(resp)
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


@app.route('/registration/', methods = ['GET', 'POST'])
def freknurRegistration():
	db = create_connection()
	try:
		resp = 'Ok'
		if(request.method == 'GET'):
			return "GET method not allowed"
		elif(request.method == 'POST'):	
			content = json.loads(request.data)
			resp = _registration_api(content,db)
			
		return str(resp)
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
loan request list.
"""
@app.route('/getLoanRequestListApi/', methods = ['GET', 'POST'])
def getLoanRequestList():
	db = create_connection()
	try:
		resp = 'Ok'
		if(request.method == 'POST'):
			return "POST method not allowed"
		elif(request.method == 'GET'):
			flag = request.args.get('flag')
			max  = request.args.get('max')
			min  = request.args.get('min')
			
			resp = _get_loan_request_list_api(flag,min,max,db)
					
		return str(resp)
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
loan payout list.
"""
@app.route('/getLoanPayoutListApi/', methods = ['GET', 'POST'])
def getLoanPayoutList():
	db = create_connection()
	try:
		resp = 'Ok'
		if(request.method == 'POST'):
			return "POST method not allowed"
		elif(request.method == 'GET'):
			flag = request.args.get('flag')
			max  = request.args.get('max')
			min  = request.args.get('min')
			
			resp = _get_loan_payout_list_api(flag,min,max,db)
					
		return str(resp)
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
-.approve loan operation.
"""
@app.route('/loanApprovalOperation/', methods = ['GET', 'POST'])
def loanApprovalOperation():
	db = create_connection()
	try:
		resp = 'Ok'
		if(request.method == 'GET'):
			return {"Result":"Ok","Message":"GET method not allowed"}		
		elif(request.method == 'POST'):
			req_data = request.get_json()
			resp = _loan_approval_api(req_data,db) 
		return jsonify({"success":True,"data":resp})		
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
-.authentication operation.
"""
@app.route('/userAuthentication/', methods = ['GET', 'POST'])
def userAuthentication():
	db = create_connection()
	try:
		resp = 'Ok'
		if(request.method == 'GET'):
			return {"Result":"Ok","Message":"GET method not allowed"}		
		elif(request.method == 'POST'):
			req_data = request.get_json()
			#resp = _loan_approval_api(req_data,db) 
		return jsonify({"success":True,"name":"admin","type":"admin"})		
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
-.exit method.
"""
def sig_exit():
	IOLoop.instance().add_callback_from_signal(do_stop)

"""
-.stop tornado method.
"""	
def do_stop():
	IOLoop.instance().stop()

"""
-.main method.
"""	
if(__name__ == '__main__'):
	try:
		http_server = HTTPServer(WSGIContainer(app))
		http_server.listen(5000)
		signal.signal(signal.SIGINT,sig_exit)
		IOLoop.instance().start()
	except KeyboardInterrupt:
		pass
	finally:
		log.info("Application is exiting...")
		IOLoop.instance().stop()
		IOLoop.instance().close(True)
		exit();
	