FROM python:3.10

RUN mkdir /app/
WORKDIR /app/
COPY adapter.py adapter.py
COPY main.py main.py
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r ./requirements.txt

# Copy entrypoint script to the image
RUN chmod +x main.py
ENV PYTHONPATH="/app/"

# Define an entrypoint script for the docker image
ENTRYPOINT ["python", "./main.py"]

CMD ["ls"]