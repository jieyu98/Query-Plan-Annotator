import streamlit as st
from contextlib import contextmanager, redirect_stdout
from io import StringIO
import preprocessing
import annotation


class Interface:
    def __init__(self):
        st.header("Query Processing")

        sql_statement = st.text_area("Type your SQL statement below")

        # When execute button is clicked
        if st.button("Execute"):
            with st.spinner('Wait for it...'):
                # Execute SQL statement and retrieve the node dicts
                qep_node_dict, aqp1_node_dict, aqp2_node_dict = preprocessing.main(sql_statement)

                # Print query plan structure
                st.subheader("Structure of the Query Execution Plan")
                structure_output = st.empty()
                self.print_query_plain(qep_node_dict, structure_output)

                # Annotate
                st.subheader("Annotation of Query Execution Plan")
                annotation_output = st.empty()
                self.print_annotations(qep_node_dict, aqp1_node_dict, aqp2_node_dict, annotation_output)

            st.success('Done!')  # Add reset button here instead maybe

    def print_query_plain(self, node_dict, output):
        with self.st_capture(output.code):
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

    def print_annotations(self, qep_node_dict, aqp1_node_dict, aqp2_node_dict, output):
        with self.st_capture(output.code):
            annotation.Annotate(qep_node_dict, aqp1_node_dict, aqp2_node_dict)

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

    @contextmanager
    def st_capture(self, output_func):
        with StringIO() as stdout, redirect_stdout(stdout):
            old_write = stdout.write

            def new_write(string):
                ret = old_write(string)
                output_func(stdout.getvalue())
                return ret

            stdout.write = new_write
            yield



