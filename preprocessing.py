from psycopg2 import connect

DEFAULT_SEQ_PAGE_COST = 1.0
DEFAULT_RAND_PAGE_COST = 5.0


class Connection:
    def __init__(self):
        print("Beginning connection")
        self.conn = self.begin_connection()
        self.cursor = self.conn.cursor()

    def begin_connection(self):
        return connect(
            dbname="TPC-H",
            user="jy",
            password="password",
            port="5432",
            host="localhost",
        )

    def kill_connection(self):
        self.conn.close()
        self.cursor.close()

    def change_parameters(self, seq_page, rand_page):
        self.cursor.execute("SET seq_page_cost TO " + str(seq_page))
        self.cursor.execute("SET random_page_cost TO " + str(rand_page))
        # self.cursor.execute("SET enable_seqscan TO " + str("False"))

    def execute_query(self, query, seq_cost, rand_cost):
        self.change_parameters(seq_cost, rand_cost)  # Set parameters of the query
        self.cursor.execute(query)  # Execute query
        query_plan = self.cursor.fetchall()  # Fetch the query plan
        return query_plan

    def get_qep(self, query):
        query_plan = self.execute_query(self.cursor.mogrify("EXPLAIN (ANALYZE, FORMAT JSON) " + query),
                                        DEFAULT_SEQ_PAGE_COST, DEFAULT_RAND_PAGE_COST)
        query_plan = query_plan[0][0][0]['Plan']
        return query_plan

    def get_aqp(self, query, disable_list):
        if 'Hash Join' in disable_list:
            self.cursor.execute('SET enable_hashjoin = off;')

        if 'Merge Join' in disable_list:
            self.cursor.execute('SET enable_mergejoin = off;')

        if 'Nested Loop' in disable_list:
            self.cursor.execute('SET enable_nestloop = off;')

        query_plan = self.execute_query(self.cursor.mogrify("EXPLAIN (ANALYZE, FORMAT JSON) " + query),
                                        DEFAULT_SEQ_PAGE_COST, DEFAULT_RAND_PAGE_COST)
        query_plan = query_plan[0][0][0]['Plan']

        # Enable them back
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

    # Recursion method
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


def main():
    query_processor = Connection()

    test_query = "SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey"
    # test_query = "SELECT n_name,sum(l_extendedprice * (1 - l_discount)) as revenue FROM customer,orders,lineitem,supplier,nation,region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' AND o_orderdate >= date '1994-01-01' AND o_orderdate < date '1994-01-01' + interval '1' year GROUP BY n_name ORDER BY revenue desc;"
    # test_query = "SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority " + \
    #              "FROM customer, orders, lineitem " + \
    #              "WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND " \
    #              "o_orderdate < date '1995-03-15' AND l_shipdate > date '1995-03-15' " + \
    #              "GROUP BY l_orderkey, o_orderdate, o_shippriority " + \
    #              "ORDER BY revenue desc, o_orderdate LIMIT 20"

    qep_node_dict = {}
    aqp1_node_dict = {}
    aqp2_node_dict = {}

    qep = query_processor.get_qep(test_query)
    query_processor.get_nodes(qep, 0, qep_node_dict)

    found = 0
    for level in qep_node_dict:
        for plan in qep_node_dict[level]:
            # Check the node type if it is a join
            if found == 0:
                if plan['Node Type'] == 'Hash Join':
                    disable_list = ['Hash Join']
                    found = 1
                elif plan['Node Type'] == 'Merge Join':
                    disable_list = ['Merge Join']
                    found = 1
                elif plan['Node Type'] == 'Nested Loop':
                    disable_list = ['Nested Loop']
                    found = 1

    print(f"AQP1 Disabled: {disable_list}")
    aqp1 = query_processor.get_aqp(test_query, disable_list)
    query_processor.get_nodes(aqp1, 0, aqp1_node_dict)

    found = 0
    for level1 in aqp1_node_dict:
        for plan1 in aqp1_node_dict[level1]:
            if found == 0:
                if plan1['Node Type'] == 'Hash Join':
                    disable_list.append('Hash Join')
                    found = 1
                elif plan1['Node Type'] == 'Merge Join':
                    disable_list.append('Merge Join')
                    found = 1
                elif plan1['Node Type'] == 'Nested Loop':
                    disable_list.append('Nested Loop')
                    found = 1

    print(f"AQP2 Disabled: {disable_list}")
    aqp2 = query_processor.get_aqp(test_query, disable_list)
    query_processor.get_nodes(aqp2, 0, aqp2_node_dict)

    return qep_node_dict, aqp1_node_dict, aqp2_node_dict