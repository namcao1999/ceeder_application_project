import datetime
import subprocess
import os

# Get the current directory of the Python file
current_dir = os.path.dirname(os.path.abspath(__file__))

# Get the current date
current_date = datetime.date.today()

# Check if it's the 15th of the month
if current_date.day == 16:
    # Run the Python file using subprocess
    subprocess.run(['python', os.path.join(current_dir, 'Outlier_Detection_Project.py')])