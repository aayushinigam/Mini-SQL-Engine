import sqlparse
import os
import csv
from itertools import product
import sys


table_info = {} #table name : table cols
table_names = [] #all tables in db
query_tables = [] #all the tables mentioned in query 
dic = {}    #cols : indices in cartesian product
aggregates = {} # col : aggregate func


#helper function for where conditions 
def evaluate(c1,op,c2) :
	if(op == '=') :
		return (c1 == c2)
	if(op == '>') :
		return (c1 > c2)
	if(op == '<') :
		return (c1 < c2)
	if(op == '>=') :
		return (c1 >= c2)
	if(op == '<=') :
		return (c1 <= c2)


#Store metadata in a dictionary from metadata.txt file -
def getMetaData() :
	flag = 0
	table_name = ''
	try :
		meta_file = open('metadata.txt','r')
		for i in meta_file :
			data = i.strip()
			if(data == '<begin_table>') :
				flag = 1
				continue
			elif (data == '<end_table>') :
				flag = 0
				continue
			elif (flag == 1) :
				table_name = data
				table_info[data] = []
				table_names.append(data)
				flag = 2
			elif (flag == 2) :
				table_info[table_name].append(data)
	except IOError as error :
		print(error)


#Process the given query 
def processQuery(query) :

	cartesian_product = []
	query_cols = []   # columns present in select
	query_table_cols = []  #all columns of the tables mentioned in query
	token_dic = {}       
	check_query = query.lower()
	where = False
	distinct = False
	group_by = False
	order_by = False
	aggregate_func = False 

	#Parse the query for further processing 
	query = sqlparse.parse(query)[0]
	tokens = query.tokens
	x = 0
	for i in tokens :
		temp = str(i).lower() 
		if('from' in temp) :
			token_dic['from'] = x
		elif('where' in temp) :
			where = True
			token_dic['where'] = x
		elif('group by' in temp) :
			group_by = True
			token_dic['groupby'] = x
		elif('order by' in temp) :
			order_by = True
			token_dic['orderby'] = x
		x += 1


	#########   ERROR HANDLING :  ############

	#check if SEMICOLON is present in query
	if(check_query[-1] != ';') :
		print("Invalid query!Please add a semicolon at the end")
		sys.exit(0)
	#check if SELECT is written in query
	if('select' not in check_query) :
		print("Incorrect Syntax!No SELECT statement")
		sys.exit(0)
	#check if FROM is written in query
	if('from' not in check_query):
		print("Incorrect Syntax!No FROM statement")
		sys.exit(0)
	#check if DISTINCT is written in query 
	if('distinct' in check_query):
		distinct = True

	#check if any AGGREGATE FUNCTION is involved 
	if(('max' in check_query or 'min' in check_query or 'average' in check_query or 'count' in check_query or 'sum' in check_query) and ('group by' not in check_query)):
		aggregate_func = True

	#store table names
	for k in str(tokens[token_dic['from'] + 2]).split(',') :
		query_tables.append(k.strip())

	#ERROR HANDLONG for tables - check if all query_tables are present - 
	for table in query_tables :
		if table not in table_names :
			print(table + " not found")
			sys.exit(0)


	#store the cols from the tables :
	for table in query_tables :
		temp = table_info[table]
		for i in temp :
			query_table_cols.append(i)
	

	#take cross product of tables
	crossProduct(query_tables,cartesian_product,dic)
	#store col names
	col = tokens[token_dic['from'] - 2]
	for k in (str(col).split(',')):
		p = str(k).lower()
		#check for aggregate functions -
		if('count' in p or 'sum' in p or 'min' in p or 'max' in p or 'average' in p) :
			s = (str(k).split('('))
			t = s[-1].split(')')[0]
			query_cols.append(t.strip())
			aggregates[t.strip()] = s[0]

		else :
			query_cols.append(str(k).strip())

	#ERROR Handling for columns
	if(len(query_cols) == 0) :
		print("No column name mentioned")
		sys.exit(0)	

	#Check if '*' is present
	if(len(query_cols) == 1) :
		#add all the query_cols to query_cols list
		if(query_cols[0] == '*') :
			query_cols = query_table_cols

	#check if given query_cols are actually present in the given tables :
	for i in query_cols :
		if(i not in query_table_cols) :
			print("Column " + i + " doesn't exist in the given tables")
			sys.exit(0)



	###########    RROCESS WHERE  ######### - 
	if(where) :

		w = token_dic['where']
		where_str = str(tokens[w])

		#Process FIRST CONDITION in where
		cond1part1 = str(tokens[w][2][0])
		op1 = str(tokens[w][2][2])
		cond1part2 = str(tokens[w][2][4])
		cond_index_list1 = []
		num_check1 = False
		num1 = 0
		if(str(cond1part1) in query_table_cols) :
			cond_index_list1.append((dic[cond1part1]))
		else :
			print("Column name in WHERE doesn't exist")
		if(str(cond1part2) in query_table_cols):
			cond_index_list1.append(((dic[cond1part2])))
		else :
			num_check1 = True
			num1 = int((cond1part2))


		#Process SECOND CONDITION in where 
		if('AND' in where_str or 'OR' in where_str) :
			cond2part1 = str(tokens[w][6][0])
			op2 = str(tokens[w][6][2])
			cond2part2 = str(tokens[w][6][4])
			cond_index_list2 = []
			num_check2 = False
			num2 = 0
			if(str(cond2part1) in query_table_cols) :
				cond_index_list2.append((dic[cond2part1]))
			else :
				print("Column name in WHERE doesn't exist")
			if(str(cond2part2) in query_table_cols):
				cond_index_list2.append(((dic[cond2part2])))
			else :
				num_check2 = True
				num2 = int((cond2part2))

		#EVALUATE CONDITIONS : 
		#store temporary result 
		temp_res_list = []

		#if the first condition is based on a constant value
		for row in cartesian_product :
				cond1_res = False
				cond2_res = False
				if(num_check1) :
					if((evaluate(row[cond_index_list1[0]],(op1),num1))) :
						cond1_res = True
				else :
					if((evaluate(row[cond_index_list1[0]],(op1),row[cond_index_list1[1]]))) :
						cond1_res = True
				if('AND' in where_str and cond1_res) :
					if(num_check2) :
						if((evaluate(row[cond_index_list2[0]],(op2),num2))) :
							cond2_res = True
					else :
						if((evaluate(row[cond_index_list2[0]],(op2),row[cond_index_list2[1]]))) :
							cond2_res = True
					if(cond2_res) :
						temp_res_list.append(row)
				if('OR' in where_str and cond1_res) :
					temp_res_list.append(row) 
				elif('OR' in where_str and cond2_res) :
					temp_res_list.append(row)
				if('AND' not in where_str and 'OR' not in where_str and cond1_res):
					temp_res_list.append(row)

		cartesian_product = temp_res_list

	


	######      PROCESS GROUP BY :   ########
	if(group_by) :
		col_index = ''
		col = str(tokens[token_dic['groupby'] + 2])
		try:
			col_index = dic[str(tokens[token_dic['groupby'] + 2])]
		except KeyError :
			print("Column in group by clause doesn't exist ")
			sys.exit(0)
		#CHECK if col in group by is present in select :
		if(col not in query_cols) :
			print(col + " must also be present in Select")
			sys.exit(0)

		temp_res = []
		ans_dict = {}
		count = 1
		#for every column in select calculate the aggregate fn value
		#store that value in a dict (ans_dict)
		for key in aggregates :
			index = dic[key]
			temp_dic = {}
			aggr = aggregates[key]
			#store key(group by col) : other column values in a dictionary 
			#to implement group by
			for i in cartesian_product :
				if(i[col_index] in temp_dic) :
					temp_dic[i[col_index]].append(i[index])
				else :
					temp_dic[i[col_index]] = []
					temp_dic[i[col_index]].append(i[index])

			#evaluate the aggregate function value :
			for i in temp_dic :
				val = 0
				if(aggr == 'min') :
					val = min(temp_dic[i])
				elif(aggr == 'max') :
					val = max(temp_dic[i])
				elif(aggr == 'count') :
					val = len(temp_dic[i]) + 1
				elif(aggr == 'sum') :
					val = sum(temp_dic[i])
				elif(aggr == 'average') :
					val = sum(temp_dic[i]) / len(temp_dic[i])

				if(i not in ans_dict) :
					ans_dict[i] = []
				ans_dict[i].append(val)
			dic[key] = count
			count += 1
		dic[query_cols[0]] = 0
		
		#store res in a list from ans_dict
		for i in ans_dict :
			temp = []
			temp.append(i)
			for j in ans_dict[i] :
				temp.append(j)
			temp_res.append(temp)
		cartesian_product = temp_res
	



	########   AGGREGATE FUNCTION ON A SINGLE COLUMN   #######
	if(aggregate_func) :
		temp_list = []
		col_index = dic[query_cols[0]]
		val = ''
		for i in cartesian_product :
			temp_list.append(i[col_index])
		if('min' in check_query):
			val = min(temp_list)
		elif('max' in check_query) :
			val = max(temp_list)
		elif('sum' in check_query):
			val = sum(temp_list)
		elif('average' in check_query) :
			val = sum(temp_list) / (len(temp_list) +1)
		elif('count' in check_query) :
			val = len(temp_list) + 1
		print(query_cols[0])
		print(val)
		sys.exit(0)
	

	########   PROCESS SELECT COLUMNS :   ########
	index_list = []
	res_list = []
	#store the indices of columns need to be printed
	for i in query_cols :
		index_list.append(dic[i])
	#store those columns in temporary list :
	for i in cartesian_product :
		n = len(i)
		temp_list = []
		for j in range(0,n) :
			if(j in index_list) :
				temp_list.append(str(i[j]))
		res_list.append(temp_list)


	###### PROCESS DISTINCT :  #######
	if(distinct) :
		temp_dic = {}
		temp_res = []
		for i in res_list :
			k = tuple(i)
			if(k in temp_dic) :
				continue
			else :
				temp_res.append(i)
				temp_dic[k] = i
		res_list = temp_res


	#####   PROCESS ORDER BY :  #####
	if(order_by) :
		col = ''
		col_index = ''
		temp = (tokens[token_dic['orderby'] + 2])
		col = str(temp[0])
		order = str(temp[-1])
		try:
			col_index = dic[col]
		except KeyError :
			print("Column in order by need to be present in group by")
			sys.exit(0)
		#sort in ascending order
		if(order.lower() == 'asc') :
			cartesian_product.sort(key = lambda x: x[col_index])
		#sort in descending order 
		else :
			cartesian_product.sort(key = lambda x: x[col_index],reverse = True)



	#####   PRINT FINAL OUTPUT #######

	#print names of columns :
	print(','.join(query_cols))
	#print comma seperated columns :
	for i in res_list :
		print(', '.join(i))



#Taking cross product of tables in the query
def crossProduct(query_tables,result_list,dic) :

	files = []
	finallist = []
	temp = 0
	for i in query_tables :
		tab = i.split('.')
		a = table_info[tab[0]]
		for j in a :
			for key in j :
				dic[key] = temp
				temp = temp + 1

	#storing each file data in a 2D list :
	'''for i in query_tables :
		fileData = []
		try :
			with open(i+'.csv','r') as f:
				reader = csv.reader(f)
				for row in reader:
					fileData.append(row)
				files.append(fileData)
		except IOError as error :
			print(error)'''

	for i in query_tables :
		fileData = []
		try :
			with open(i+'.csv','r') as f:
				reader = csv.reader(f)
				for row in reader:
					temp_list = row
					for i in temp_list :
						stripped = i.strip()
						if(stripped.startswith('"') and stripped.endswith('"')) :
							print("inside if")
							temp_list[i] = int(temp_list[i])
					fileData.append(temp_list)
				files.append(fileData)
		except IOError as error :
			print(error)

	#taking the product
	finallist = list(product(*files))

	#storing the product in a 2D list
	for i in finallist :
		temp = []
		for j in i :
			for k in j :
				temp.append(int(k))
		result_list.append(temp)



#Main function 
if __name__ == '__main__' :
	getMetaData()
	query = sys.argv[1]
	processQuery(query)

	







	
