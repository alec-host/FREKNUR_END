#!/usr/bin/python

import os
import sys


from db_helper import _record_loan_request_db,_loan_request_list_db,_loan_payout_list_db,\
                     _loan_approval_operation_db,_get_loan_request_db,_mark_loan_request_processed_db,\
					 _queue_loan_db,_registration_db,_get_loan_payout_db,_dispatch_loan_db,_record_loan_transaction_db,\
					 _record_loan_fee_db,_statement_db,_get_accounts_db,_get_debtor_list_db,_get_defaulter_list_db,_get_account_summary_db,\
					 _mpesa_receipt_db

def _record_loan_request_api(json,conn=None):
	db_response = 'None'
	if(json is not None):
		"""
		-.extract json values.
		"""
		msisdn = json['msisdn']
		amount = json['amount']
		"""
		-.write loan request to db.
		"""
		db_response = _record_loan_request_db(msisdn,amount,conn)
		
	return db_response 


def _registration_api(json,conn=None):
	db_response = 'None'
	if(json is not None):
		"""
		-.extract json values.
		"""
		msisdn = json['msisdn']
		passwd = json['passwd']
		"""
		-.write loan request to db.
		"""
		db_response = _registration_db(msisdn,passwd,conn)
		
	return db_response 
	
def _get_statement_api(msisdn,conn=None):
	db_response = 'None'
	if(msisdn is not None):
		db_response = _statement_db(msisdn,conn)
	
	return db_response

def _get_loan_request_list_api(search,min,max,conn=None):
	db_response = 'None'
	if(conn is not None):
		db_response = _loan_request_list_db(search,min,max,conn)
	
	return db_response

def _get_loan_payout_list_api(search,min,max,conn=None):	
	db_response = 'None'
	if(conn is not None):
	
		db_response = _loan_payout_list_db(search,min,max,conn)
		
	return db_response

def _get_accounts_log_api(code,search,min,max,conn=None):
	if(conn is not None):
		db_response = _get_accounts_db(code,search,min,max,conn)		
	
	return db_response
	
def _get_debtor_list_api(search,min,max,conn=None):
	if(conn is not None):
		db_response = _get_debtor_list_db(search,min,max,conn)		
	
	return db_response

def _get_defaulter_list_api(search,min,max,conn=None):
	if(conn is not None):
		db_response = _get_defaulter_list_db(search,min,max,conn)		
	
	return db_response
	
def _get_account_summary_api(min,max,conn=None):
	if(conn is not None):
		db_response = _get_account_summary_db(min,max,conn)		
	
	return db_response	
	
def _loan_approval_api(data,conn=None):
	db_response = 'None'
	loan_id = data.get("id",None)
	user = data.get("user",None)
	mobile = data.get("mobile",None)
	if(data is not None):
		db_response = _loan_approval_operation_db(loan_id,mobile,user,conn)
	
	return db_response

def _mpesa_receipt_api(data,conn=None):
    db_response = None
	msisdn = data.get("msisdn",None)
	amount = data.get("amount",None)
	if(conn is not None and msisdn is not None):
		db_response = _mpesa_receipt_db(msisdn,amount,conn)
	
	return db_response
	
def _read_loan_request_queue_sys(conn=None):
	if(conn is not None):
		db_response = _get_loan_request_db(conn)		
	
	return db_response
	
def _mark_loan_request_processed_sys(_id,msisdn,ref_no,conn=None):
	if(conn is not None and _id is not None and msisdn is not None and ref_no is not None):
		db_response = _mark_loan_request_processed_db(_id,msisdn,ref_no,conn)		
	
	return db_response
	
def _read_loan_payout_queue_sys(conn=None):
	if(conn is not None):
		db_response = _get_loan_payout_db(conn)		
	
	return db_response	
	
def _queue_loan_sys(reference_no,msisdn,loan_amount,approved_by,conn=None):
	if(conn is not None and reference_no is not None and msisdn is not None and loan_amount is not None):
		db_response = _queue_loan_db(reference_no,msisdn,loan_amount,approved_by,conn)
		
	return db_response
	
def _dispatch_loan_sys(reference_no,msisdn,amount,loan_amount,repayment_amount,interest_amount,loan_duration,notify_1,conn=None):
	if(conn is not None and reference_no is not None and msisdn is not None and amount is not None and loan_amount is not None and repayment_amount is not None and interest_amount is not None):
		db_response = _dispatch_loan_db(reference_no,msisdn,amount,loan_amount,repayment_amount,interest_amount,loan_duration,notify_1,conn)
		
	return db_response
	
def _record_loan_transaction_sys(amount,msisdn,reference_no,account_name,transaction_type,conn=None):
	if(conn is not None and amount is not None and msisdn is not None and reference_no is not None and account_name is not None and transaction_type is not None):
		db_response = _record_loan_transaction_db(amount,msisdn,reference_no,account_name,transaction_type,conn)
		
	return db_response

def _record_loan_fee_sys(msisdn,reference_no,loan_fee,account_name,conn=None):
	if(conn is not None and msisdn is not None and reference_no is not None and loan_fee is not None and account_name is not None):
		db_response = _record_loan_fee_db(msisdn,reference_no,loan_fee,account_name,conn)
		
	return db_response
	