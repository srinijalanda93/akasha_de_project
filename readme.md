<h1> Akasha Air Data Engineering Project</h1>

    <p>Hello! This is a complete data pipeline built for the Akasha Air data engineering task.</p>

    <p>The whole goal of this project is to take messy data from different places, clean it up, load it into a database, and get useful business answers from it.</p>

    <hr>

    <h2>What This Project is <em>Really</em> About</h2>

    <p>This entire task is a test for one main concept: <strong>ETL (Extract, Transform, Load)</strong>.</p>

    <p>They want to see if we can build a small, automated "data factory."</p>
    <ul>
        <li><strong>Extract:</strong> Can we <em>read</em> data from two different file types? (In our case, one <code>.csv</code> and one <code>.xml</code> file).</li>
        <li><strong>Transform:</strong> Can we <em>clean</em> this data? The files aren't linked by <code>customer_id</code>, so we have to find the <em>real</em> link (<code>mobile_number</code>) to combine them and get new information.</li>
        <li><strong>Load:</strong> Can we <em>load</em> this clean, combined data into a proper database (MySQL)?</li>
        <li><strong>Analyze:</strong> Can we <em>answer</em> business questions (like "who are our top customers?") using this new, clean data?</li>
    </ul>

    <p>This project does all four of those things. It also answers the questions in two different ways (using SQL and using Python's Pandas library) to show both methods work.</p>

    <hr>

    <h2>Core Features</h2>

    <ul>
        <li><strong>Extracts:</strong> Reads customer data from CSV and order data from XML.</li>
        <li><strong>Transforms:</strong>
            <ul>
                <li>Cleans and normalizes the order data (which was one order per <em>item</em>) into a clean list of <em>unique orders</em>.</li>
                <li>Converts text-based dates into real <code>datetime</code> objects so we can do time-based math (like "last 30 days").</li>
            </ul>
        </li>
        <li><strong>Loads:</strong> Loads the clean data into three normalized tables in a MySQL database: <code>customers</code>, <code>orders</code>, and <code>order_items</code>.</li>
        <li><strong>Analyzes (2 Ways):</strong>
            <ol>
                <li><strong>Approach A (SQL):</strong> Runs pure SQL queries against the database to get all 4 KPIs.</li>
                <li><strong>Approach B (Pandas):</strong> Runs in-memory analysis using Python to get the <em>same</em> 4 KPIs.</li>
            </ol>
        </li>
        <li><strong>Secure:</strong> Keeps database credentials safe in a hidden <code>.env</code> file.</li>
        <li><strong>Modular Code:</strong> The code is not one giant file! It's broken into logical modules for maintainability (see below).</li>
    </ul>

    <hr>

    <h2>📂 How the Code is Organized</h2>

    <p>Instead of one giant, messy <code>main.py</code> file, the code is broken into logical "modules" (separate files) in the <code>pipeline/</code> folder. This makes it super easy to read, test, and maintain.</p>

    <pre><code>akasha-de-project/
│
├── pipeline/
│   ├── db_connector.py     # Handles the database connection
│   ├── data_processor.py   # Handles Extracting & Transforming data
│   ├── sql_analysis.py     # Handles Requirement A (SQL)
│   └── pandas_analysis.py  # Handles Requirement B (Pandas)
│
├── main.py                 # The "main" script! It just imports
│                           # the modules and runs them in order.
│
├── data/
│   ├── task_DE_new_customers.csv
│   └── task_DE_new_orders.xml
│
├── exploration_notebook.ipynb  # The notebook used to explore the data
├── .env                        # (You must create this!)
├── .gitignore                  # Hides .env and other junk from GitHub
└── requirements.txt            # All the libraries to install
</code></pre>

    <hr>

    <h2>🛠️ Technology Stack</h2>

    <ul>
        <li><strong>Language:</strong> Python 3</li>
        <li><strong>Core Libraries:</strong>
            <ul>
                <li><code>pandas</code>: For all data reading, cleaning, and in-memory analysis.</li>
                <li><code>SQLAlchemy</code>: For connecting Python to the MySQL database safely.</li>
                <li><code>mysql-connector-python</code>: The specific "driver" for MySQL.</li>
                <li><code>lxml</code>: The high-performance parser that <code>pandas</code> needs to read XML.</li>
                <li><code>python-dotenv</code>: For loading our secure credentials from the <code>.env</code> file.</li>
            </ul>
        </li>
        <li><strong>Database:</strong> MySQL</li>
    </ul>

    <hr>

    <h2>⚙️ How to Run This Project</h2>

    <p>You only need 5 things: <strong>Python</strong>, <strong>MySQL</strong>, the <strong>code</strong>, your <strong>credentials</strong>, and the <strong>libraries</strong>.</p>

    <h3>Step 1: Get the Code & Data</h3>
    <p>Clone this project onto your computer. Make sure the <code>data/</code> folder has your two data files.</p>

    <h3>Step 2: Set Up MySQL</h3>
    <p>Make sure your MySQL server is <strong>running</strong> (the green light!). In MySQL Workbench, create a new, empty schema:</p>
    <pre><code>CREATE SCHEMA akasha_db;</code></pre>

    <h3>Step 3: Create Your Password File (Crucial!)</h3>
    <p>In the main project folder (next to <code>main.py</code>), create a file named <strong><code>.env</code></strong>. Copy and paste this inside, and add your password:</p>
    <pre><code>DB_USER="root"
DB_PASS="your_password"
DB_HOST="localhost"
DB_PORT="3306"
DB_NAME="akasha_db"
</code></pre>

    <h3>Step 4: Install the Libraries</h3>
    <p>Open your terminal in the project folder and run:</p>
    <pre><code>pip install -r requirements.txt</code></pre>

    <h3>Step 5: Run the Pipeline!</h3>
    <p>That's it. Just run the <code>main.py</code> file from your terminal.</p>
    <pre><code>python main.py</code></pre>
    <p>You will see the script print its progress, starting with "Successfully connected to MySQL database," and then it will print out all the final KPI reports.</p>

    <hr>

    <h2> What I Would Add Next (Future Improvements)</h2>

    <p>This pipeline works great for a daily run, but a real-world project needs a few more things. This section answers the prompt's request for "additional features or improvements."</p>

    <ul>
        <li><strong>Schedule It:</strong> Use a tool like <strong>Apache Airflow</strong> to make this script run automatically every single day, so the data is always fresh.</li>
        <li><strong>Add Data Checks:</strong> Use a library like <strong>Great Expectations</strong> to automatically check the data <em>before</em> loading it. For example, it could check if <code>mobile_number</code> is always 10 digits or if <code>total_amount</code> is always a positive number.</li>
        <li><strong>Add Logging:</strong> Instead of just <code>print()</code>, I'd use Python's <code>logging</code> module to write all successes and errors to a log file, which is much better for debugging.</li>
    </ul>

    