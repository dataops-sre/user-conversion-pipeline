### same python version spark image
FROM python:3.12 as builder

WORKDIR /mnt

COPY . /mnt

RUN apt-get update && apt-get install -y zip

RUN zip -r dependencies.zip jobs \
    -x "*/__pycache__/*" \
    -x "*/tests/*"

### same spark image as our juyter-notebook
FROM quay.io/jupyter/pyspark-notebook:spark-3.5.2

WORKDIR /workspace

# copy Python dependecies
COPY --from=builder /mnt/dependencies.zip /workspace/dist/dependencies.zip
# copy Spark jobs
COPY ./jobs/ /workspace/jobs/

# provide entrypoint file
COPY ./scripts/entrypoint.sh /workspace/scripts/entrypoint.sh

CMD ["/workspace/scripts/entrypoint.sh"]
