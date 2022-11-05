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

    def get_aqp1(self, query):
        query_plan = self.execute_query(self.cursor.mogrify("EXPLAIN (ANALYZE, FORMAT JSON) " + query),
                                        DEFAULT_SEQ_PAGE_COST + 5, DEFAULT_RAND_PAGE_COST + 5)
        query_plan = query_plan[0][0][0]['Plan']
        return query_plan

    def get_aqp2(self, query):
        query_plan = self.execute_query(self.cursor.mogrify("EXPLAIN (ANALYZE, FORMAT JSON) " + query),
                                        DEFAULT_SEQ_PAGE_COST + 10, DEFAULT_RAND_PAGE_COST + 10)
        query_plan = query_plan[0][0][0]['Plan']
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

    aqp1 = query_processor.get_aqp1(test_query)
    query_processor.get_nodes(aqp1, 0, aqp1_node_dict)

    aqp2 = query_processor.get_aqp2(test_query)
    query_processor.get_nodes(aqp2, 0, aqp2_node_dict)
    return qep_node_dict, aqp1_node_dict, aqp2_node_dict