# Use the AWS Lambda-provided base image for Python 3.8
FROM public.ecr.aws/lambda/python:3.8

# Set the working directory
WORKDIR /var/task

# Copy function code and install dependencies
COPY app.py .
COPY requirements.txt .

# Install dependencies
RUN pip3 install -r requirements.txt

# Command for Lambda to execute your function
CMD ["app.lambda_handler"]