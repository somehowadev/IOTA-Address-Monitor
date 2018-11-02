from iota import Iota
from iota.crypto.addresses import AddressGenerator
from datetime import datetime
from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity
from azure.servicebus import ServiceBusService, Message, Queue
import iota
import json
import random
import time



def iotaapi(node):
	api = Iota(node)
		
	return(api)

def checknodestate(api):
	api = api
	nodeinfo = api.get_node_info()
	latestmilestone = (nodeinfo['latestMilestoneIndex'])
	latestsubtanglemilestone = (nodeinfo ['latestSolidSubtangleMilestoneIndex'])
	print(str(latestmilestone) + " " + str(latestsubtanglemilestone))
	if latestmilestone - latestsubtanglemilestone > 3:
		return 2
	else:
		return 1

def findtransactions(address, api):
	ar = []
	hash = []
	search = address 
	ar.append(search)
	txcount = 0
	api = api
	txs = api.find_transactions(bundles = None,
								addresses = ar,
								tags = None,
								approvees = None)
	for js in txs['hashes']:
		txcount += 1 
		hash.append(repr(js))
	return(hash)

def tableservice():
	with open('config.json', 'r') as f:
		config = json.load(f)
	account_key = config['STORAGE']['ACCOUNT_NAME'] # Azyre Storage Account name
	secret_key = config['STORAGE']['SECRET_KEY'] # Azure Storage Account Key
	table_key = config['STORAGE']['TABLE_KEY'] # Azure table name
	table_service = TableService(account_name=account_key, account_key=secret_key)
	return(table_service)

def getaddresses(table):
	addresses = []
	with open('config.json', 'r') as f:
		config = json.load(f)
	table_key = config['STORAGE']['TABLE_KEY'] # Azure table name
	entities = table.query_entities(table_key)
	for walletaddr in entities:
		addresses.append(walletaddr.PartitionKey)
	return(addresses)

def getbalance(address,api):
	addressarr = []
	addressarr.append(address)
	balance = api.get_balances(addressarr)
	return(balance)

def comparebalance(address, strippedbalance, api, table):
	with open('config.json', 'r') as f:
		config = json.load(f)
	table_key = config['STORAGE']['TABLE_KEY'] # Azure table name
	# To test with your own pretend balances to compare, use the balance variblae
	# To use with real values on the tangle, use the addresses.balance method.
	balance = 0.0

	#try:
	#	add_address = (table.insert_entity(table_key,task))	

	#except Exception as AzureConflictHttpError:
	get_address = (table.query_entities(table_key, "PartitionKey eq '" + address +  "'"))
	for addresses in get_address:
		comparison = strippedbalance != addresses.balance
		if comparison:
			updatebalance(addresses.PartitionKey,addresses.RowKey,strippedbalance,table)
		print(comparison)
		return(comparison)	

def queuehandler(table):
	with open('config.json', 'r') as f:
		config = json.load(f)
	count = 0
	table_key = config['STORAGE']['QUEUE_TABLE_KEY'] # Azure table name
	

	queue_namespace = config['QUEUE']['NAMESPACE']
	namespace_key = config['QUEUE']['ACCOUNT_KEY']
	SASName = config['QUEUE']['SASNAME']
	queue_name = config['QUEUE']['QUEUE_NAME']
	bus_service = ServiceBusService(service_namespace=queue_namespace,
									shared_access_key_value=namespace_key,
									shared_access_key_name=SASName)
	try:
		queue_options = Queue()
		queue_options.max_size_in_megabytes = '5120'
		queue_options.default_message_time_to_live = 'PT8H'
		queue_options.requires_duplicate_detection = True
		#queue_options.requires_session = True
		bus_service.create_queue('email_notification', queue_options)
	except Exception as e: 
		f = open("exceptions.txt", "a")
		f.write(e) 
		print("Exception occured in Service Bus Handler, check log")
	return(bus_service)

def queuesender(bus_service, address, balance, table, TX):
	with open('config.json', 'r') as f:
		config = json.load(f)
	queue_name = config['QUEUE']['QUEUE_NAME']
	account_key = config['STORAGE']['ACCOUNT_NAME'] # Azyre Bus Service Account name
	secret_key = config['STORAGE']['SECRET_KEY'] # Azure Bus Service Account Key
	table_key = config['STORAGE']['QUEUE_TABLE_KEY'] # Azure table name

	count = 0

	#txstring = [repr(transaction.replace('[TransactionHash(b', "").replace("]","")) for transaction in TX]
	txstring = [transaction.replace('(b', ": ").replace('\')"',"") for transaction in TX]
	
	jsonmessage = {"Address": address, "Balance": balance, "TXHash": txstring}
	jsonparsed = json.dumps(jsonmessage)

	msg = Message(jsonparsed)
	#msg.broker_properties = '{"SessionId": ' + '"{' + str(count) + '}"'
	bus_service.send_queue_message(queue_name,msg)
	
	countup = int(count) + 1
	print(countup)
	task = {'PartitionKey':'Queue', 'RowKey':'SessionID', 'ID':countup}


def updatebalance(address,email,strippedbalance,table):
	with open('config.json', 'r') as f:
		config = json.load(f)
	table_key = config['STORAGE']['TABLE_KEY']
	
	task = {'PartitionKey':address, 'RowKey':email, 'balance':strippedbalance}
	try:
		update_balance = (table.update_entity(table_key, task))
	except Exception as e: 
		f = open("exceptions.txt", "a")
		f.write(e) 
		print("Exception occured in updating balance, check log")
		
if __name__ == "__main__":
	api = iotaapi("https://field.deviota.com:443")
	nodesync = checknodestate(api)

	if nodesync == 1:
		print("Node is healthy")
		status = 1
	else:
		print("Node is probably out of sync")
		time.sleep(300)

	while status == 1:

		azurecon = tableservice()
		#Pass Azure connection details to address finder
		addresses = getaddresses(azurecon)
		# Each address, check balance
		for address in addresses:
			bl = getbalance(address,api)
			#Strip the results of useless brackets
			strippedbalance = str(bl['balances']).replace("[", "").replace("]","")
			strippedbalance = int(strippedbalance) / 1000000
			setb = comparebalance(address, strippedbalance, api, azurecon)
			if setb == True:
				PreviousTX = findtransactions(address, api)
				handler = queuehandler(azurecon)
				print(address)
				queuesender(handler, address, strippedbalance, azurecon, PreviousTX)
				for tx in PreviousTX:
					print(tx)
		
		status = 1
		