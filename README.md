# Query Plan Annotator

This was created as part of a graded coursework for NTU's CZ4031 Database System Principles.

The aim of this project is to design an algorithm which can take in a SQL Query in Postgres, retrieve its Query Execution Plan (QEP) and its Alternative Query Plans (AQPs), and returns as output an annotated SQL Query which describes the execution of the various components of the query.

## Installation Guide

### Setting up the environment
To run the project, we will first need to install all the dependencies used. In the Windows CMD, navigate to the root directory of the project folder and run the following:
1. `pip install psycopg2`
2. `pip install streamlit`
3. `pip install pandas`
4. `pip install altair`

### Changing the database credentials
Next, we will need to change the database credentials. They can be configured in the *preprocessing.py* file, line 4 to line 8.

### Running Streamlit
Finally, to launch the web application we built using Streamlit, run the following in the Windows CMD.

`streamlit run project.py`

You should now see the graphical user interface launched in your web browser.
