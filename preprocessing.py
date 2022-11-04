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
                                        DEFAULT_SEQ_PAGE_COST+3, DEFAULT_RAND_PAGE_COST+1)
        query_plan = query_plan[0][0][0]['Plan']
        return query_plan

    def get_aqp2(self, query):
        query_plan = self.execute_query(self.cursor.mogrify("EXPLAIN (ANALYZE, FORMAT JSON) " + query),
                                        DEFAULT_SEQ_PAGE_COST+5, DEFAULT_RAND_PAGE_COST+2)
        query_plan = query_plan[0][0][0]['Plan']
        return query_plan

    # Recursion method
    def get_nodes(self, query_plan):
        if 'Plans' in query_plan.keys():
            for plan in query_plan["Plans"]:
                print(plan['Node Type'])
                self.get_nodes(plan)

    # Iterative method
    def get_nodes(self, query_plan):
        level = 0


def main():
    # test_query = "SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey LIMIT 10"
    test_query = "SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority " +\
    "FROM customer, orders, lineitem " +\
    "WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < date '1995-03-15' AND l_shipdate > date '1995-03-15' " +\
    "GROUP BY l_orderkey, o_orderdate, o_shippriority " +\
    "ORDER BY revenue desc, o_orderdate LIMIT 20"

    qep = query_processor.get_qep(test_query)
    query_processor.get_nodes(qep)
    # aqp1 = query_processor.get_aqp1(test_query)
    # aqp2 = query_processor.get_aqp2(test_query)

    print(qep)
    # print(aqp1)
    # print(aqp2)

    return


query_processor = Connection()
main()
