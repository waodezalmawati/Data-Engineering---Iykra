FROM python:3.9

# Install nano editor just in case we need to write some file
RUN apt-get update 
RUN apt-get -y install nano 

# Install the python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy all the files in the existing folder into /usr/src/app
WORKDIR D:/TKJ/IYKRA/DataLake/iykra-task1
COPY . .

# Export google application credentials to have the necessary permission
ENV GOOGLE_APPLICATION_CREDENTIALS="credentials.json"



# FROM python:3.9.6
# FROM google
# WORKDIR .
# COPY task1.py .
# CMD ["python", "task1.py"]