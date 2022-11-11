from psycopg2 import connect

# Specify DB credentials here
DBNAME = "TPC-H"
USER = "jy"
PASSWORD = "password"
PORT = "5432"
HOST = "localhost"

DEFAULT_SEQ_PAGE_COST = 1.0
DEFAULT_RAND_PAGE_COST = 5.0


class Preprocessor:
    def __init__(self):
        print("Beginning connection")
        self.conn = self.begin_connection()
        self.cursor = self.conn.cursor()

    # To begin connection
    def begin_connection(self):
        return connect(
            dbname=DBNAME,
            user=USER,
            password=PASSWORD,
            port=PORT,
            host=HOST,
        )

    # To stop connection
    def kill_connection(self):
        self.conn.close()
        self.cursor.close()

    # To change sequential and random scan costs of a query
    def change_parameters(self, seq_page, rand_page):
        self.cursor.execute("SET seq_page_cost TO " + str(seq_page))
        self.cursor.execute("SET random_page_cost TO " + str(rand_page))

    # Executes a query to retrieve the query plan
    def execute_query(self, sql_statement, seq_cost, rand_cost):
        self.change_parameters(seq_cost, rand_cost)  # Set parameters of the query
        self.cursor.execute(sql_statement)  # Execute query
        query_plan = self.cursor.fetchall()  # Fetch the query plan
        return query_plan

    # Calls execute_query function to get qep
    def get_qep(self, sql_statement):
        query_plan = self.execute_query(self.cursor.mogrify("EXPLAIN (ANALYZE, FORMAT JSON) " + sql_statement),
                                        DEFAULT_SEQ_PAGE_COST, DEFAULT_RAND_PAGE_COST)
        query_plan = query_plan[0][0][0]['Plan']
        # print(query_plan)
        query_plan = self.modify_costs(query_plan)  # Modify costs of joins
        return query_plan

    # Change total costs of joins from cumulative to just the cost of the join
    def modify_costs(self, query_plan):
        cost_of_children = 0
        if 'Plans' not in query_plan:  # reached a leaf node already
            return
        for child in query_plan['Plans']:
            cost_of_children += child['Total Cost']
        query_plan['Node Cost'] = round(query_plan['Total Cost'] - cost_of_children, 4)
        for child in query_plan['Plans']:  # now repeat the process for the child
            self.modify_costs(child)
        return query_plan

    # Function to call execute_query to get an aqp by disabling joins in disable_list
    def get_aqp(self, sql_statement, disable_list):
        if 'Hash Join' in disable_list:
            self.cursor.execute('SET enable_hashjoin = off;')

        if 'Merge Join' in disable_list:
            self.cursor.execute('SET enable_mergejoin = off;')

        if 'Nested Loop' in disable_list:
            self.cursor.execute('SET enable_nestloop = off;')

        query_plan = self.execute_query(self.cursor.mogrify("EXPLAIN (ANALYZE, FORMAT JSON) " + sql_statement),
                                        DEFAULT_SEQ_PAGE_COST, DEFAULT_RAND_PAGE_COST)
        query_plan = query_plan[0][0][0]['Plan']
        query_plan = self.modify_costs(query_plan)  # Modify costs of joins

        # Enable everything back
        self.cursor.execute('SET enable_nestloop = on;')
        self.cursor.execute('SET enable_mergejoin = on;')
        self.cursor.execute('SET enable_hashjoin = on;')

        return query_plan

    def process_plan(self, plan):
        temp = {}

        for i in plan:
            if i != 'Plans':
                temp[i] = plan[i]
        return temp

    # Get all the nodes in the query plan - Recursion method
    def get_nodes(self, query_plan, level, node_dict):
        if level == 0:
            node_dict[level] = [self.process_plan(query_plan)]

        if 'Plans' in query_plan.keys():
            level += 1
            for plan in query_plan['Plans']:
                # Check if level already exists
                if level in node_dict:
                    # Append to the existing array if level already exists
                    node_dict[level].append(self.process_plan(plan))
                else:
                    node_dict[level] = [self.process_plan(plan)]

                self.get_nodes(plan, level, node_dict)

    def get_disable_list(self, disable_list, node_dict):
        found = 0
        for level in node_dict:
            for plan in node_dict[level]:
                # Check the node type if it is a join
                if found == 0:
                    if plan['Node Type'] == 'Hash Join':
                        if len(disable_list) > 0:
                            disable_list.append('Hash Join')
                        else:
                            disable_list = ['Hash Join']
                        found = 1
                    elif plan['Node Type'] == 'Merge Join':
                        if len(disable_list) > 0:
                            disable_list.append('Merge Join')
                        else:
                            disable_list = ['Merge Join']
                        found = 1
                    elif plan['Node Type'] == 'Nested Loop':
                        if len(disable_list) > 0:
                            disable_list.append('Nested Loop')
                        else:
                            disable_list = ['Nested Loop']
                        found = 1

        return disable_list


def main(sql_statement):
    query_processor = Preprocessor()

    qep_node_dict = {}
    aqp1_node_dict = {}
    aqp2_node_dict = {}

    # Get qep and populate qep_node_dict
    qep = query_processor.get_qep(sql_statement)
    query_processor.get_nodes(qep, 0, qep_node_dict)

    disable_list = []

    # Get disable list for aqp1
    disable_list = query_processor.get_disable_list(disable_list, qep_node_dict)
    print(f"AQP1 Disabled: {disable_list}")

    # Get aqp1 and populate aqp1_node_dict
    aqp1 = query_processor.get_aqp(sql_statement, disable_list)
    query_processor.get_nodes(aqp1, 0, aqp1_node_dict)

    # Get disable list for aqp2
    disable_list = query_processor.get_disable_list(disable_list, aqp1_node_dict)
    print(f"AQP2 Disabled: {disable_list}")

    # Get aqp2 and populate aqp2_node_dict
    aqp2 = query_processor.get_aqp(sql_statement, disable_list)
    query_processor.get_nodes(aqp2, 0, aqp2_node_dict)

    return qep_node_dict, aqp1_node_dict, aqp2_node_dict
