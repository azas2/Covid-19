
<h2>🔧 Project Details</h2>

<h3>Key Components</h3>
<ol>
    <li><strong>Apache Airflow DAG</strong>:
        <ul>
            <li>The Directed Acyclic Graph (DAG) orchestrates the entire ETL process.</li>
            <li>It defines the sequence of tasks and their dependencies.</li>
        </ul>
    </li>
    <li><strong>Data Sources</strong>:
        <ul>
            <li>CSV files stored in a specified directory.</li>
            <li>A PostgreSQL database table for storing the transformed data.</li>
        </ul>
    </li>
    <li><strong>Tasks in the DAG</strong>:
        <ul>
            <li><strong>Extract Task</strong>: Reads raw data from CSV files.</li>
            <li><strong>Transform Task</strong>: Cleans and enriches the data (e.g., replacing <code>NaN</code> values and ensuring correct data types).</li>
            <li><strong>Load Task</strong>: Loads the transformed data into the PostgreSQL database.</li>
        </ul>
    </li>
    <li><strong>PostgreSQL Database</strong>:
        <ul>
            <li>Stores the final dataset for analytical queries.</li>
            <li>Includes tables such as <code>date_dim</code>, <code>fact_cases</code>, etc.</li>
        </ul>
    </li>
    <li><strong>Tools and Libraries</strong>:
        <ul>
            <li><code>pandas</code>: For data manipulation and cleaning.</li>
            <li><code>psycopg2</code>: For PostgreSQL database interaction.</li>
            <li><code>StringIO</code>: For efficient in-memory data transfer.</li>
        </ul>
    </li>
</ol>



<h2>📺 DAG Workflow</h2>
<ol>
    <li><strong>Start</strong>: Initialize the pipeline.</li>
    <li><strong>Extract</strong>: Read data from the CSV source.</li>
    <li><strong>Transform</strong>: Clean and preprocess the data.</li>
    <li><strong>Load</strong>: Insert the transformed data into the target database table.</li>
    <li><strong>End</strong>: Mark the DAG as successfully completed.</li>
</ol>


<h2>🖼️ Images</h2>

<h3>1. Example Run of the DAG</h3>
<img src="path/to/run_dag_image.png" alt="Run DAG" style="max-width:100%;">

<h3>2. Flowchart of the Project Workflow</h3>
<img src="path/to/flowchart_image.png" alt="Flowchart" style="max-width:100%;">


<h2>🔧 Usage Instructions</h2>

<h3>Prerequisites</h3>
<ul>
    <li>Install Apache Airflow.</li>
    <li>Ensure PostgreSQL is set up and accessible.</li>
    <li>Have the required Python libraries installed:</li>
</ul>
<pre><code>pip install pandas psycopg2</code></pre>

<h3>Steps to Run the Project</h3>
<ol>
    <li><strong>Start Airflow</strong>:
        <pre><code>airflow scheduler & airflow webserver</code></pre>
    </li>
    <li><strong>Upload the DAG</strong>: Place the DAG file in the Airflow <code>dags</code> directory.</li>
    <li><strong>Prepare the Database</strong>:
        <ul>
            <li>Run the SQL scripts to create necessary tables (e.g., <code>date_dim</code>, <code>fact_cases</code>).</li>
        </ul>
    </li>
    <li><strong>Trigger the DAG</strong>:
        <ul>
            <li>Go to the Airflow UI.</li>
            <li>Locate the DAG and click <strong>Trigger DAG</strong>.</li>
        </ul>
    </li>
</ol>


<h2>🚫 Troubleshooting</h2>

<h3>Common Errors</h3>
<ol>
    <li><strong>Invalid Data Format</strong>: Ensure all CSV files have consistent formatting and correct data types.</li>
    <li><strong>Database Connection Issues</strong>: Check the PostgreSQL connection details in the DAG.</li>
    <li><strong>Missing Dependencies</strong>: Verify all required Python libraries are installed.</li>
</ol>

<h3>Logs</h3>
<p>Check Airflow task logs for detailed debugging information.</p>


<h2>📧 Contact</h2>
<p>For further assistance, please contact:</p>
<ul>
    <li><strong>Name</strong>: [Your Name]</li>
    <li><strong>Email</strong>: [Your Email]</li>
</ul>
