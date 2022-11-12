import pandas as pd
import altair as alt


class Annotate:
    def __init__(self, qep_node_dict, aqp1_node_dict, aqp2_node_dict, sql_statement):
        self.qep_node_dict = qep_node_dict
        self.aqp1_node_dict = aqp1_node_dict
        self.aqp2_node_dict = aqp2_node_dict

        # Extract joins info from node dicts
        self.qep_join_info = self.extract_join_info(qep_node_dict)
        self.aqp1_join_info = self.extract_join_info(aqp1_node_dict)
        self.aqp2_join_info = self.extract_join_info(aqp2_node_dict)

        # Extract scan info from qep node dict
        self.qep_scan_info = self.extract_scan_info(qep_node_dict)

    # Create time chart for data visualization (Compares actual total time)
    def create_time_chart(self):
        source = pd.DataFrame({
            'Actual Total Time (ns)': [self.aqp2_node_dict[0][0]['Actual Total Time'],
                                       self.aqp1_node_dict[0][0]['Actual Total Time'],
                                       self.qep_node_dict[0][0]['Actual Total Time']],
            'Query plan': ['AQP2', 'AQP1', 'QEP']
        })

        bar_chart = alt.Chart(source).mark_bar().encode(
            y='Actual Total Time (ns)',
            x=alt.X('Query plan', sort='y'),
        ).properties(
            title='Time of each query plan'
        )

        return bar_chart

    # Create cost chart for data visualization (Compares total cost)
    def create_cost_chart(self):
        source = pd.DataFrame({
            'Total Cost': [self.aqp2_node_dict[0][0]['Total Cost'], self.aqp1_node_dict[0][0]['Total Cost'],
                           self.qep_node_dict[0][0]['Total Cost']],
            'Query plan': ['AQP2', 'AQP1', 'QEP']
        })

        bar_chart = alt.Chart(source).mark_bar().encode(
            y='Total Cost',
            x=alt.X('Query plan', sort='y'),
        ).properties(
            title='Cost of each query plan'
        )

        return bar_chart

    #
    def annotate_tables(self, qep_scan_info):
        # For each plan, print the plan name and its reasons
        for table in qep_scan_info:
            print(f"{table[0]} is performed on {table[1]}")
            main_explanation = self.get_annotation(table[0])
            print("\t", main_explanation)
            print('\n')

    def annotate_joins(self, qep_join_info, aqp1_join_info, aqp2_join_info):
        for join in qep_join_info:
            print(f"{join[0]} is performed on conditions {join[1:-1]}")
            main_explanation = self.get_annotation(join[0])
            print("\t", main_explanation)
            difference, conds = self.compare_join_infos_for_annot(join, aqp1_join_info, 'AQP1')
            difference2, conds2 = self.compare_join_infos_for_annot(join, aqp2_join_info, 'AQP2')
            if difference:
                print("\t", difference)
                node_differences1 = self.get_node_diff_reasons(join[0], aqp1_join_info[0][0])
                print("\t", node_differences1)
            if difference2:
                print("\t", difference2)
                node_differences2 = self.get_node_diff_reasons(join[0], aqp2_join_info[0][0])
                print("\t", node_differences2)
            print('\n')

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
                if plan['Node Type'] == 'Seq Scan' or plan['Node Type'] == 'Index Scan' or plan['Node Type'] == 'Bitmap Scan':
                    temp = [plan['Node Type']]  # Append node type first
                    temp.append(plan['Relation Name'])
                    scan_info.append(temp)

        return scan_info

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

    @staticmethod
    def strip_table_name(cond):
        """accepts 'nation.n_nationkey', converts to 'n_nationkey' for easier comparison"""
        fixed = []
        for key in cond:
            if '.' in key:
                key = key.split('.')[1]
            fixed.append(key)
        return fixed

    def compare_join_infos_for_annot(self, qep_join, aqp_join_info, aqp_name):
        # aqp_name is either 'AQP1' or 'AQP2'
        for aqp_join in aqp_join_info:
            diff_join_type = qep_join[0] != aqp_join[0]  # Boolean to check if join conditions differ
            # Need to check if the number of join conditions is the same
            if len(qep_join) == len(aqp_join):  # Same number of join conditions
                for i in range(1, len(qep_join) - 1):  # Loop through each join condition
                    qep_cond = self.strip_table_name(qep_join[i])
                    aqp_cond = self.strip_table_name(aqp_join[i])
                    same_cond = qep_cond[0] in aqp_cond and qep_cond[1] in aqp_cond
                    if not same_cond:
                        break
                else:
                    if diff_join_type:
                        if aqp_join[-1] > qep_join[-1]:
                            cost_diff = round(100 - (qep_join[-1] / aqp_join[-1]) * 100, 3)
                            difference = qep_join[0] + ' was used in the QEP because as compared to ' + aqp_join[
                                0] + f' in {aqp_name}, the {qep_join[0]} reduced cost by {cost_diff}% (from {aqp_join[-1]} to {qep_join[-1]})'
                            return difference, qep_join[1:-1]
        return None, None

    def get_annotation(self, type):
        main_explaination = "NIL"

        # Scans
        if type == 'Seq Scan':
            main_explaination = "Sequential scan is done because there is no index on the table data or when fetching " \
                                "a few rows from a large table."

        if type == 'Index Scan':
            main_explaination = "Index Scan is done as there is an index on the tables, thus index scan having lower " \
                                "cost as compared to Sequential Scan."

        if type == 'Bitmap Scan':
            main_explaination = "Bitmap Scan is used as the middle ground between index scan and sequential scan, " \
                                "where the number of records chosen may be too much for index scan but too little for" \
                                " sequential scan."

        if type == 'Index Only Scan':
            main_explaination = "Index scan will access data only through the index and not the table, reducing IO " \
                                "cost, this is used because there is no need to access table data, only index data, " \
                                "for the results."

        if type == 'Subquery Scan':
            main_explaination = "Subquery Scan will scan through the results from a child query."

        # Joins
        if type == 'Hash Join':
            main_explaination = "Hash join is used as there is a hash table created in one of the tables."

        if type == 'Merge Join':
            main_explaination = "Merge Join is used because the tables are sorted and uses minimal memory."

        if type == 'Nested Loop':
            main_explaination = "Nested loop join is used when the records to be looked for is small and the joined " \
                                "columns of the inner row source are uniquely indexed."

        return main_explaination

    def get_node_diff_reasons(self, qep_node, aqp_node):
        text = ""

        if qep_node == "Index Scan" and aqp_node['Node Type'] == "Seq Scan":
            text = "\tIndex Scan is more efficient because Sequential Scan will scan over the entire table."

        if qep_node == "Seq Scan" and aqp_node == "Index Scan":
            text = "\tGiven the index scan's higher per row cost and the low selectivity of the scan predicate, sequential scan would be a better option to use compared to index scan due to lower cost."

        if qep_node== "Index Scan" and aqp_node == "Bitmap Scan":
            text = "\tSince the scan predicate, {qep_node[Index Condition]}, has high selectivity, it is more preferrable to use Index Scan instead of Bitmap Scan."

        if qep_node == "Bitmap Scan" and aqp_node == "Index Scan":
            text = "\tSince the scan predicate, {qep_node[Index Condition]}, has low selectivity,Bitmap scan is preferred to Index Scan."

        if qep_node == "Merge Join" and aqp_node == "Nested Loop":
            text = "\tSince the relations to be joined are already sorted, Merge Join would be preferable over Nested Loop."

        if qep_node == "Nested Loop" and aqp_node == "Merge Join":
            text = "\tSince the outer loop relation is relatively small and all tuples with the same join attribute values cannot fit into memory, nested loop will be more cost efficient than merge join."

        if qep_node == "Merge Join" and aqp_node == "Hash Join":
            text = "\tGiven that the hash table does not fit into the memory, hash join becomes slower and less preferrable compared to merge join."

        if qep_node == "Hash Join" and aqp_node == "Merge Join":
            text = "\tHash table can fit into memory, thus reducing the hash join cost, making it more preferrable than the merge join."

        if qep_node == "Hash Join" and aqp_node == "Nested Loop":
            text = "\tHash table can fit into memory, reducing cost of hash join, thus making it better compared to nested loop."

        if qep_node == "Nested Loop" and aqp_node == "Hash Join":
            text = "\tOne of the operand has very few rows. Making nested loop more cost efficient compared to hash join."

        return text

