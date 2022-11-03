from psycopg2 import connect

class Connection:
    def __init__(self):
        print("Hello world")
        self.conn = self.begin_connection()
        self.cursor = self.conn.cursor()

    def begin_connection(self):
        """
            Begin connection with the Postgresql database
            Returns:
                connect: connection to database
        """
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

    def execute_query(self, query, seq_cost=1, rand_cost=1):
        # seq_cost and rand_cost are set by default to 1 in the above params
        # no need to set in get_qep
        self.change_parameters(seq_cost, rand_cost)
        self.cursor.execute(query)
        query_plan = self.cursor.fetchall()
        return query_plan

    def get_qep(self, query):
        query_plan = self.execute_query(self.cursor.mogrify("EXPLAIN (ANALYZE, FORMAT JSON) " + query))
        query_plan = query_plan[0][0][0]['Plan']
        return query_plan

    def get_aqp_1(self, query):
        query_plan = self.execute_query(self.cursor.mogrify("EXPLAIN (ANALYZE, FORMAT JSON) " + query), 1, 3)
        query_plan = query_plan[0][0][0]['Plan']
        return query_plan

    def get_aqp_2(self, query):
        query_plan = self.execute_query(self.cursor.mogrify("EXPLAIN (ANALYZE, FORMAT JSON) " + query), 1, 0)
        query_plan = query_plan[0][0][0]['Plan']
        return query_plan

def main():
    # below query is only for testing
    output = query_processor.get_qep(
        "SELECT *" +
        "FROM customer C, orders O " +
        "WHERE " +
        "C.c_custkey = O.o_custkey " +
        "LIMIT 10"
    )
    print(output)
    return


query_processor = Connection()
main()
