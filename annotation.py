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
            main_explaination = "Nested loop is used when the records to be looked for is small and the joined columns of the inner row source are uniquely indexed."

        if node['Node Type'] == 'Index Only Scan':
            main_explaination = "Index scan will access data only through the index and not the table, reducing IO cost, this is used because there is no need to access table data, only index data, for the results"
            additional_info = f"Index scan involved {node['Plan Rows']} rows"

        if node['Node Type'] == 'Subquery Scan':
            main_explaination = "Subquery Scan will scan through the results from a child query"

        return main_explaination, additional_info

    def print_plan_plain(self, node_dict):
        total_cost = 0
        for i in node_dict:
            total_cost += node_dict[i][0]['Total Cost']
            if i == 0:
                node = node_dict[i][0]
                print(f"{node['Node Type']}")
            else:
                if len(node_dict[i]) == 1:
                    node = node_dict[i][0]
                    for j in range(0, i):
                        print("\t", end='')
                    print("└── ", f"{node['Node Type']}")
                else:
                    # More than 1 plans
                    #  if one of these nodes also have 'More than 1 plans', then not sure if display will be okay?
                    for j in range(0, i):
                        print("\t", end='')
                    node = node_dict[i][0]
                    print("├── ", f"{node['Node Type']}")

                    for j in range(0, i):
                        print("\t", end='')
                    node = node_dict[i][1]
                    print("└── ", f"{node['Node Type']}")

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

                if(len(node_dict[i]) > 1):
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

    annotation = Annotate()
    print("                                                            QUERY PLAN STRUCTURE                                                            ")
    print("--------------------------------------------------------------------------------------------------------------------------------------------")
    annotation.print_plan_plain(qep_node_dict)
    print("                                                                 QUERY PLAN                                                                 ")
    print("--------------------------------------------------------------------------------------------------------------------------------------------")
    annotation.print_plan_with_annotation(qep_node_dict)
    print("                                                              AQP 1 STRUCTURE                                                               ")
    print("--------------------------------------------------------------------------------------------------------------------------------------------")
    annotation.print_plan_plain(aqp1_node_dict)
    print("                                                              AQP 2 STRUCTURE                                                               ")
    print("--------------------------------------------------------------------------------------------------------------------------------------------")
    annotation.print_plan_plain(aqp2_node_dict)

    