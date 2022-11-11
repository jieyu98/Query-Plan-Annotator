import streamlit as st
from contextlib import contextmanager, redirect_stdout
from io import StringIO
import preprocessing
import annotation


class Interface:
    def __init__(self):
        st.set_page_config(page_title="CZ4031 Group", layout="wide")
        st.title("Query Processing")

        sql_statement = st.text_area("Type your SQL statement below")

        # When execute button is clicked
        if st.button("Execute"):
            with st.spinner('Wait for it...'):
                # Execute SQL statement and retrieve the node dicts
                qep_node_dict, aqp1_node_dict, aqp2_node_dict = preprocessing.main(sql_statement)

                # Print query plan structure
                st.header("Structure of the Query Execution Plan")
                structure_output = st.empty()
                self.print_query_plain(qep_node_dict, structure_output)

                # Annotate
                st.header("Annotation of Query Execution Plan")
                st.subheader("Types of Scans Used")
                table_output = st.empty()
                st.subheader("Types of Joins Used")
                join_output = st.empty()

                # Display graphs
                st.header("Data Visualization")
                col1, col2 = st.columns(2)

                annotator = annotation.Annotate(qep_node_dict, aqp1_node_dict, aqp2_node_dict, sql_statement)
                time_data = annotator.create_time_chart()
                cost_data = annotator.create_cost_chart()

                with col1:
                    st.altair_chart(time_data, use_container_width=True)

                with col2:
                    st.altair_chart(cost_data, use_container_width=True)

                self.print_annotations(annotator, join_output, table_output)


            st.success('Done!')  # Add reset button here instead maybe

        # if st.button('Reset'):
        #     with st.spinner('Resetting...'):



    def print_query_plain(self, node_dict, output):
        with self.st_capture(output.code):
            total_cost = 0
            for i in node_dict:
                if i == 0:
                    node = node_dict[i][0]
                    print(f"{node['Node Type']} {self.get_main_details(node)}")
                else:
                    if len(node_dict[i]) == 1:
                        node = node_dict[i][0]
                        for j in range(0, i):
                            print("  ", end='')
                        print("└── ", f"{node['Node Type']} {self.get_main_details(node)}")
                    else:
                        # More than 1 plans
                        for j in range(0, i):
                            print("  ", end='')
                        node = node_dict[i][0]
                        print("├── ", f"{node['Node Type']} {self.get_main_details(node)}")

                        for j in range(0, i):
                            print("  ", end='')
                        node = node_dict[i][1]
                        print("└── ", f"{node['Node Type']} {self.get_main_details(node)}")

            print(f"Total Cost: {node_dict[0][0]['Total Cost']}")
            print(f"Total Time: {node_dict[0][0]['Actual Total Time']}")

    def print_annotations(self, annotator, join_output, table_output):
        with self.st_capture(table_output.code):
            annotator.annotate_tables(annotator.qep_scan_info)
            print(annotator.qep_scan_info)

        with self.st_capture(join_output.code):
            annotator.annotate_joins(annotator.qep_join_info, annotator.aqp1_join_info, annotator.aqp2_join_info)


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



