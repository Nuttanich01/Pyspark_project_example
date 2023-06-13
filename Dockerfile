FROM python:3.9-slim-buster
# Set environment variables
ENV PYSPARK_PYTHON=/usr/local/bin/python3
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:/usr/local/bin:$JAVA_HOME

# Insatll dependencies for psycog2
RUN apt-get update && apt-get install -y libpq-dev
RUN apt-get install -y build-essential

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant 

# Download PostgreSQL JDBC driver
RUN apt-get update && apt-get install -y curl
RUN mkdir -p /usr/share/java/
RUN curl -L -o /usr/share/java/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Install dependencies
COPY pyproject.toml poetry.lock ./
RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-dev

# Copy the script and env
COPY main.py ./
COPY .env ./