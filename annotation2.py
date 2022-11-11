from preprocessing import main


class Annotate:
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


    def extract_join_info(self, query_plan):
        join_info = []

        # Loop through each level
        for level in query_plan:
            # Loop through each plan
            for plan in query_plan[level]:
                # Search for joins - Hash join, nested loop, merge join
                if plan['Node Type'] == 'Hash Join':
                    temp = [plan['Node Type']]  # Append node type first

                    info = self.cond_split(plan['Hash Cond'])
                    for i in range(len(info)):  # Append join info
                        temp.append(info[i])

                    join_info.append(temp)

                if plan['Node Type'] == 'Merge Join':
                    temp = [plan['Node Type']]  # Append node type first

                    info = self.cond_split(plan['Merge Cond'])
                    for i in range(len(info)):  # Append join info
                        temp.append(info[i])

                    join_info.append(temp)

                if plan['Node Type'] == 'Nested Loop':
                    if 'Join Filter' in plan:
                        temp = [plan['Node Type']]  # Append node type first

                        info = self.cond_split(plan['Join Filter'])

                        for i in range(len(info)):  # Append join info
                            temp.append(info[i])

                        join_info.append(temp)
                    else:  # Case where the Nested Loop join does not have a Join Filter attribute
                        conds = self.retrieve_child_index_scan_conds(query_plan, level)  # Get from child

                        temp = [plan['Node Type']]  # Append node type first

                        info = self.cond_split(conds)
                        for i in range(len(info)):  # Append join info
                            temp.append(info[i])

                        join_info.append(temp)

        return join_info

    def retrieve_child_index_scan_conds(self, query_plan, level):
        for i in range(1, 3):
            for plan in query_plan[level + i]:
                if plan['Node Type'] == 'Index Scan':
                    return plan['Index Cond']

class main():
    qep_node_dict, aqp1_node_dict, aqp2_node_dict = main()

    annotation = Annotate()
    qep_join_info = annotation.extract_join_info(qep_node_dict)
    print("QEP")
    print(qep_join_info)
    aqp1_join_info = annotation.extract_join_info(aqp1_node_dict)
    print("AQP1")
    print(aqp1_join_info)
    aqp2_join_info = annotation.extract_join_info(aqp2_node_dict)
    print("AQP2")
    print(aqp2_join_info)

    for qep_join in qep_join_info:
        for aqp1_join in aqp1_join_info:
            diff_join_type = qep_join[0] != aqp1_join[0]
            same_keys = qep_join[1][0] in aqp1_join[1] and qep_join[1][1] in aqp1_join[1]
            if diff_join_type and same_keys:
                print(qep_join, ' in the QEP was changed to ', aqp1_join, ' in AQP1')





main()