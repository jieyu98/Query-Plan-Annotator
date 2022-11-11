class Annotate:
    def __init__(self, qep_node_dict, aqp1_node_dict, aqp2_node_dict, sql_statement):
        # Extract joins info from node dicts
        qep_join_info = self.extract_join_info(qep_node_dict)
        aqp1_join_info = self.extract_join_info(aqp1_node_dict)
        aqp2_join_info = self.extract_join_info(aqp2_node_dict)

        # Extract scan info from qep node dict
        qep_scan_info = self.extract_scan_info(qep_node_dict)

        # Perform annotations
        self.annotate_joins(qep_join_info)
        self.annotate_tables(qep_scan_info)

    def annotate_tables(self, qep_scan_info):
        # For each plan, print the plan name and its reasons
        for table in qep_scan_info:

            print(f"{table[0]} is performed on {table[1]}")
            main_explanation, additional_info = self.get_annotation(table[0], table[1])
            print("\t", main_explanation)
            print("\t", additional_info)

    def annotate_joins(self, qep_join_info):
        # For each plan, print the plan name and its reasons
        # need to add the 'This join is implemented using hash join
        #  operator as NL joins and merge join
        #  increase the estimated cost by at least 10
        #  and 7 times, respectively.'
        for join in qep_join_info:
            print(f"{join[0]} is performed on conditions {join[1:-1]}")
            main_explanation, additional_info = self.get_annotation(join[0], join[1:-1])
            print("\t", main_explanation)
            print("\t", additional_info)
    def cond_split(self, original_string):
        posAND = original_string.find('AND')

        if posAND >= 0:  # found AND
            newString = original_string[:posAND - 1] + ' = ' + original_string[posAND + 4:]
            newString = newString.replace('(', '')
            newString = newString.replace(')', '')

            list = newString.split(' = ')

            return [[list[0], list[1]], [list[2], list[3]]]
        else:  # no AND
            # sample string "(customer.c_nationkey = supplier.s_nationkey)"
            newString = original_string.replace('(', '')
            newString = newString.replace(')', '')

            list = newString.split(' = ')

            return [[list[0], list[1]]]

    def extract_scan_info(self, node_dict):
        scan_info = []

        for level in node_dict:
            for plan in node_dict[level]:
                # If
                if plan['Node Type'] == 'Seq Scan' or plan['Node Type'] == 'Index Scan' or plan[
                    'Node Type'] == 'Bitmap Scan':
                    temp = [plan['Node Type']]  # Append node type first
                    temp.append(plan['Relation Name'])
                    scan_info.append(temp)

        return scan_info

    def append_table_anno_to_sql_query(self, table_info, sql_statement):
        # will have to be run only after the joins are appended, otherwise formatting messup
        sql_statement = format_sql_commands(sql_statement)  # need to do first for this
        index_of_FROM = sql_statement.find('FROM')
        tables_substring = sql_statement[index_of_FROM:].split("\n")[0]
        original_length_table_substring = len(tables_substring)
        for table in table_info:
            scan_type = table[0]
            table_name = table[1]
            index_to_append = tables_substring.find(table_name)
            temp_list = list(tables_substring)
            temp_list.insert(index_to_append + len(table_name), f' (we used am ffkjsdlkjfslkdjfdffkjs dlkjfslkdjfdffkjsdlkjfslk jfdffkjsdlkjfslkdjfdffkjsdlkjfslk djfdffkjsdlkjfslkdjfdffkjsdlkjfslkdjf dffkjsdlkjfslkdjfdffkjsdlkjfslkdjfd  {scan_type})')
            tables_substring = ''.join(temp_list)
        tables_list = tables_substring.split(',')
        new_tables_string = tables_list[0]
        for statement in tables_list[1:]:
            new_tables_string += "\n      " + statement
        # sql_statement = sql_statement[:index_of_FROM] \
        #                 + new_tables_string \
        #                 + sql_statement[index_of_FROM + original_length_table_substring:]
        return new_tables_string

    def append_joins_anno_to_sql_query(self, join_info, sql_statement):
        # temporary
        sql_statement = sql_statement.lower()
        for join in join_info:
            join_name = join[0]
            join_cost = join[-1]
            join_conds = join[1:-1]
            for join_cond in join_conds:
                key1 = join_cond[0].split('.')[1].lower()
                key2 = join_cond[1].split('.')[1].lower()
                substring1 = f"{key1} = {key2}"
                substring2 = f"{key2} = {key1}"
                index_to_append = sql_statement.find(substring1) if substring1 in sql_statement \
                    else sql_statement.find(substring2)
                index_to_append += len(substring1)
                temp_list = list(sql_statement)
                temp_list.insert(index_to_append, f' (Join implemented using hash join operator because NL joins, merge join increase the estimated cost by at least 10, 7 times, respectively.)')
                sql_statement = ''.join(temp_list)
        return format_sql_commands(sql_statement)

    def extract_join_info(self, node_dict):
        join_info = []

        # Loop through each level
        for level in node_dict:
            # Loop through each plan
            for plan in node_dict[level]:
                # Search for joins - Hash join, nested loop, merge join
                if plan['Node Type'] == 'Hash Join':
                    temp = [plan['Node Type']]  # Append node type first

                    info = self.cond_split(plan['Hash Cond'])
                    for i in range(len(info)):  # Append join conditions
                        temp.append(info[i])
                    temp.append(plan['Node Cost'])

                    join_info.append(temp)

                if plan['Node Type'] == 'Merge Join':
                    temp = [plan['Node Type']]  # Append node type first

                    info = self.cond_split(plan['Merge Cond'])
                    for i in range(len(info)):  # Append join conditions
                        temp.append(info[i])
                    temp.append(plan['Node Cost'])

                    join_info.append(temp)

                if plan['Node Type'] == 'Nested Loop':
                    if 'Join Filter' in plan:
                        temp = [plan['Node Type']]  # Append node type first

                        info = self.cond_split(plan['Join Filter'])

                        for i in range(len(info)):  # Append join conditions
                            temp.append(info[i])
                        temp.append(plan['Node Cost'])

                        join_info.append(temp)
                    else:  # Case where the Nested Loop join does not have a Join Filter attribute
                        conds = self.retrieve_child_index_scan_conds(node_dict, level)  # Get from child

                        temp = [plan['Node Type']]  # Append node type first

                        info = self.cond_split(conds)
                        for i in range(len(info)):  # Append join conditions
                            temp.append(info[i])
                        temp.append(plan['Node Cost'])

                        join_info.append(temp)

        return join_info

    def retrieve_child_index_scan_conds(self, node_dict, level):
        for i in range(1, 3):
            for plan in node_dict[level + i]:
                if plan['Node Type'] == 'Index Scan' and 'Index Cond' in plan:
                    return plan['Index Cond']


    ##########################################################################################################

    def get_annotation(self, type, extra_info):
        # extra info will be join conditions if we're passing in a join
        # extra info will be a table name if we're passing in a scan
        main_explaination = "NIL"
        additional_info = ""

        # Scans
        if type == 'Seq Scan':
            main_explaination = "Sequential scan is done because there is no index on the table data or when fetching a few rows from a large table"
            additional_info = f"Sequential Scan is performed on the relation: {extra_info}"

        if type == 'Index Scan':
            main_explaination = "Index Scan is done as there is an index on the tables, thus index scan having lower cost as compared to Sequential Scan"
            additional_info = f"Index Scan is done on the conditions {extra_info}"

        if type == 'Bitmap Scan':
            main_explaination = "Bitmap Scan is used as the middle ground between index scan and sequential scan, where the number of records chosen may be too much for index scan but too little for sequential scan"

        if type == 'Index Only Scan':
            main_explaination = "Index scan will access data only through the index and not the table, reducing IO cost, this is used because there is no need to access table data, only index data, for the results"
            additional_info = f"Index scan involved {extra_info} rows"

        if type == 'Subquery Scan':
            main_explaination = "Subquery Scan will scan through the results from a child query"

        # Joins
        if type == 'Hash Join':
            main_explaination = "Hash join is used as there is a hash table created in one of the tables. "
            additional_info = f"Hash Join is used on the condition {node['Hash Cond']}"

        if node['Node Type'] == 'Seq Scan':
            main_explaination = "Sequential scan is done because there is no index on the table data or when fetching a few rows from a large table"
            additional_info = f"Sequential Scan is performed on the relation: {node['Relation Name']}"

        if node['Node Type'] == 'Hash':
            main_explaination = "Hash is used due to the table is the smaller one and thus minimal memory is required to store the hash table in memory, or for a future hash join"
            additional_info = f"Hash used {node['Hash Buckets']} buckets"

        if node['Node Type'] == 'Merge Join':
            main_explaination = "Merge Join is used because the tables are sorted and uses minimal memory"
            additional_info = f"Merge join is used on the condition {extra_info}"

        if type == 'Nested Loop':
            main_explaination = "Nested loop join is used when the records to be looked for is small and the joined columns of the inner row source are uniquely indexed."

        if node['Node Type'] == 'Index Only Scan':
            main_explaination = "Index scan will access data only through the index and not the table, reducing IO cost, this is used because there is no need to access table data, only index data, for the results"
            additional_info = f"Index scan involved {node['Plan Rows']} rows"

        if node['Node Type'] == 'Subquery Scan':
            main_explaination = "Subquery Scan will scan through the results from a child query"

        return main_explaination, additional_info

    # def node_diff_reasons(node_dict, node_dict2):
    # for i in node_dict:
    #   text = ""
    #   node = node_dict[i][0]
    #   node2 = node_dict2[i][0]
    #
    #   if node['Node Type'] == "Index Scan" and node2['Node Type'] == "Seq Scan":
    #       text = f"Difference Reasoning: "
    #       text += f"QEP uses Index Scan on relation {node['Relation Name']} while AQP uses Sequential Scan on relation {node2['Relation Name']} "
    #       text += "Sequential Scan is used to scan over the entire table, which is less efficient as compared to Index Scan "
    #
    #   if node['Node Type'] == "Seq Scan" and node2['Node Type'] == "Index Scan":
    #       text = f"Difference Reasoning: "
    #       text += f"QEP uses Sequential Scan on relation {node['Relation Name']} while AQP uses Index Scan on relation {node2['Relation Name']} "
    #       text += "Given the index scan's higher per row cost and the low selectivity of the scan predicate, sequential scan would be a better option to use compared to index scan due to lower cost"
    #
    #   if node['Node Type'] == "Index Scan" and node2['Node Type'] == "Bitmap Scan":
    #       text = f"Difference Reasoning: "
    #       text += f"QEP uses Index Scan on relation {node['Relation Name']} while AQP uses Bitmap Scan on relation {node2['Relation Name']} "
    #       text += "Since the scan predicate, {node[Index Condition]}, has high selectivity, it is more preferrable to use Index Scan instead of Bitmap Scan"
    #
    #   if node['Node Type'] == "Bitmap Scan" and node2['Node Type'] == "Index Scan":
    #       text = f"Difference Reasoning: "
    #       text += f"QEP uses Bitmap Scan on relation {node['Relation Name']} while AQP uses Index Scan on relation {node2['Relation Name']} "
    #       text += "Since the scan predicate, {node[Index Condition]}, has low selectivity,Bitmap scan is preferred to Index Scan"

    #   if node['Node Type'] == "Merge Join" and node2['Node Type'] == "Nested Loop":
    #       text = f"Difference Reasoning: "
    #       text += f"QEP uses Nested Loop on relation {node['Relation Name']} while AQP uses Merge Join on the relation {node2['Relation Name']} "
    #       text += "Since the relations to be joined are already sorted, Merge Join would be preferrable over Nested Loop"
    #
    #   if node['Node Type'] == "Nested Loop" and node2['Node Type'] == "Merge Join":
    #      text = f"Difference Reasoning: "
    #      text += f"QEP uses Merge Join on relation {node['Relation Name']} while AQP uses Nested Loop on the relation {node2['Relation Name']} "
    #      text += "Since the outer loop relation is relatively small and all tuples with the same join attribute values cannot fit into memory, nested loop will be more cost efficient than merge join."
    #
    #   if node['Node Type'] == "Merge Join" and node2['Node Type'] == "Hash Join":
    #      text = f"Difference Reasoning: "
    #      text += f"QEP uses Merge Join on relation {node['Relation Name']} while AQP uses Hash Join on the relation {node2['Relation Name']} "
    #      text += "Given that the hash table does not fit into the memory, hash join becomes slower and less preferrable compared to merge join."

    #  if node['Node Type'] == "Hash Join" and node2['Node Type'] == "Merge Join":
    #      text = f"Difference Reasoning: "
    #      text += f"QEP uses Hash Join on relation {node['Relation Name']} while AQP uses Merge Join on the relation {node2['Relation Name']} "
    #      text += "Hash table can fit into memory, thus reducing the hash join cost, making it more preferrable than the merge join."

    #   if node['Node Type'] == "Hash Join" and node2['Node Type'] == "Nested Loop":
    #      text = f"Difference Reasoning: "
    #      text += f"QEP uses Hash Join on relation {node['Relation Name']} while AQP uses Nested Loop on the relation {node2['Relation Name']} "
    #      text += "Hash table can fit into memory, reducing cost of hash join, thus making it better compared to nested loop."

    #   if node['Node Type'] == "Nested Loop" and node2['Node Type'] == "Hash Join":
    #      text = f"Difference Reasoning: "
    #      text += f"QEP uses Nested Loop on relation {node['Relation Name']} while AQP uses Hash Join on the relation {node2['Relation Name']} "
    #      text += "One of the operand has very few rows. Making nested loop more cost efficient compared to hash join."

    def compare_join_infos(self, qep_join_info, aqp_join_info, aqp_name):
        # aqp_name is either 'AQP1' or 'AQP2'
        differences_list = []
        for qep_join in qep_join_info:
            for aqp_join in aqp_join_info:
                diff_join_type = qep_join[0] != aqp_join[0]  # Boolean to check if join conditions differ
                # Need to check if the number of join conditions is the same
                if len(qep_join) == len(aqp_join):  # Same number of join conditions
                    for i in range(1, len(qep_join) - 1):  # Loop through each join condition
                        qep_cond = qep_join[i]
                        aqp_cond = aqp_join[i]
                        same_cond = qep_cond[0] in aqp_cond and qep_cond[1] in aqp_cond
                        if not same_cond:
                            break
                    else:
                        if diff_join_type:
                            cost_diff = round((aqp_join[-1] / qep_join[-1]) * 100, 4)
                            if cost_diff < 1:
                                cost_phrasing = f' and the qep join reduced cost by {cost_diff}'
                            else:
                                cost_phrasing = f' and the aqp join reduced cost by {cost_diff}'
                            differences_list.append(
                                [qep_join[0], ' in the QEP was changed to ', aqp_join[0], f' in {aqp_name}'
                                                                                          f'{cost_phrasing}'])
        return differences_list


# import preprocessing
#
sql_statement = "SELECT c_custkey,c_name,sum(l_extendedprice * (1 - l_discount)) as revenue,c_acctbal,n_name,c_address,c_phone,c_comment FROM customer,orders,lineitem,nation WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= date '1993-10-01' AND o_orderdate < date '1993-10-01' + interval '3' month AND l_returnflag = 'R' AND c_nationkey = n_nationkey GROUP BY c_custkey,c_name,c_acctbal,c_phone,n_name,c_address,c_comment ORDER BY revenue desc LIMIT 20;"
qep_node_dict, aqp1_node_dict, aqp2_node_dict = preprocessing.main(sql_statement)
annotator = Annotate(qep_node_dict, aqp1_node_dict, aqp2_node_dict, sql_statement)


