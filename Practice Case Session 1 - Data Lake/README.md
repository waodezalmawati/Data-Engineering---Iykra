# iykra-task1-docker-upload-file-to-GCP

This repository contains files for uploading image files from the internet to Google Cloud Platform using Docker and Python.

## Prerequisites :
1. Google Cloud Platform Account (https://console.cloud.google.com/)
2. Docker Desktop (https://www.docker.com/products/docker-desktop/)
3. Python 3.6 or later (https://www.python.org/downloads/)
4. Code Editor (in my case I user VS Code) (https://code.visualstudio.com/download)

## How To Run :
1. Make sure you have enabled the IAM API on your Google Cloud, and created your service account on the google cloud. You can read about it here https://cloud.google.com/iam/docs/creating-managing-service-accounts
2. After successfully creating a service account, download the ```.json``` file containing your private key
2. Copy the contents of the .json file that you downloaded, and paste it into the ```.env``` file
3. Open your code editor, and run the command ```docker build -t image-name .``` (the dot is also entered in the terminal)
4. Create the container by running the command ```docker run -it --rm --name container-name image-name /bin/bash```
5. Run the command python ```file-name.py```
6. Finally, refresh the bucket on your GCP

# Good luck!
