from preprocessing import main


class Annotate:
    def get_annotation(self, node):
        main_explaination = "NIL"
        additional_info = ""

        if node['Node Type'] == 'Limit':
            main_explaination = "Limit is used because only a certain number of rows are required"

        if node['Node Type'] == 'Hash Join':
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
            additional_info = f"Merge join is used on the condition {node['Condition']}"

        if node['Node Type'] == 'Sort':
            main_explaination = "Sort is done for a merge-join to carry out in the later stages"
            additional_info = f"Sort is done on the key {node['Sort Key']}"

        if node['Node Type'] == 'Index Scan':
            main_explaination = "Index Scan is done as there is an index on the tables, thus index scan having lower cost as compared to Sequential Scan"
            additional_info = f"Index Scan is done on the conditions {node['Index Cond']}" \
                              f"Index Scan is done using filter {node['Filter']}" \
                              f"There were {node['Rows Removed by Filter']} rows removed by the filter"

        if node['Node Type'] == 'Bitmap Scan':
            main_explaination = "Bitmap Scan is used as the middle ground between index scan and sequential scan, where the number of records chosen may be too much for index scan but too little for sequential scan"

        if node['Node Type'] == 'Aggregate':
            main_explaination = "the results from this query plan are aggregated, due to the group-by function"
            additional_info = f"The aggregation is done on the keys {node['Group Key']}"

        if node['Node Type'] == 'Nested Loop':
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

    def get_main_details(self, node):
        if node['Node Type'] == 'Seq Scan' or \
                node['Node Type'] == 'Index Scan':
            return "(" + node['Relation Name'] + ")"

        if node['Node Type'] == 'Hash Join':
            return node['Hash Cond']

        if node['Node Type'] == 'Merge Join':
            return node['Merge Cond']

        if node['Node Type'] == 'Nested Loop':
            if 'Join Filter' in node:
                return node['Join Filter']
            # else
            # look one level down to find a child with node type index scan
            # get index cond
            # if _pkey in child_name
            #

        return ""

    def print_plan_plain(self, node_dict):
        total_cost = 0
        for i in node_dict:
            total_cost += node_dict[i][0]['Total Cost']
            if i == 0:
                node = node_dict[i][0]
                print(f"{node['Node Type']} {self.get_main_details(node)}")
            else:
                if len(node_dict[i]) == 1:
                    node = node_dict[i][0]
                    for j in range(0, i):
                        print("\t", end='')
                    print("└── ", f"{node['Node Type']} {self.get_main_details(node)}")
                else:
                    # More than 1 plans
                    #  if one of these nodes also have 'More than 1 plans', then not sure if display will be okay?
                    for j in range(0, i):
                        print("\t", end='')
                    node = node_dict[i][0]
                    print("├── ", f"{node['Node Type']} {self.get_main_details(node)}")

                    for j in range(0, i):
                        print("\t", end='')
                    node = node_dict[i][1]
                    print("└── ", f"{node['Node Type']} {self.get_main_details(node)}")

        print(f"Total Cost: {total_cost}")
        print(f"Total Time: {node_dict[0][0]['Actual Total Time']}")

    def print_plan_with_annotation(self, node_dict):
        for i in node_dict:
            if i == 0:
                node = node_dict[i][0]
                main_explaination, additional_info = self.get_annotation(node)
                print(f"\033[1m{node['Node Type']}\033[0m ({main_explaination})")
            else:
                node = node_dict[i][0]
                for j in range(0, i):
                    print("\t", end='')
                main_explaination, additional_info = self.get_annotation(node)
                print("->  ", f"\033[1m{node['Node Type']}\033[0m ({main_explaination})")
                for j in range(0, i):
                    print("\t", end='')
                print(f"     {additional_info}")

                if (len(node_dict[i]) > 1):
                    for j in range(0, i):
                        print("\t", end='')
                    node = node_dict[i][1]
                    main_explaination, additional_info = self.get_annotation(node)
                    print("->  ", f"\033[1m{node['Node Type']}\033[0m ({main_explaination})")
                    for j in range(0, i):
                        print("\t", end='')
                    print(f"     {additional_info}")


class main():
    qep_node_dict, aqp1_node_dict, aqp2_node_dict = main()
    # print(qep_node_dict)
    # print(aqp1_node_dict)
    annotation = Annotate()
    print("Note: run annotation2.py for now")
    # print(
    #     "                                                            QUERY PLAN STRUCTURE                                                            ")
    # print(
    #     "--------------------------------------------------------------------------------------------------------------------------------------------")
    # annotation.print_plan_plain(aqp2_node_dict)
    # # print("                                                                 QUERY PLAN                                                                 ")
    # # print("--------------------------------------------------------------------------------------------------------------------------------------------")
    # # annotation.print_plan_with_annotation(qep_node_dict)
    # print("                                                              AQP 1 STRUCTURE                                                               ")
    # print("--------------------------------------------------------------------------------------------------------------------------------------------")
    # annotation.print_plan_plain(aqp1_node_dict)
    # print("                                                              AQP 2 STRUCTURE                                                               ")
    # print("--------------------------------------------------------------------------------------------------------------------------------------------")
    # annotation.print_plan_plain(aqp2_node_dict)
