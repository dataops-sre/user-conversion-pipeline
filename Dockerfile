### same python version spark image
FROM python:3.10.6 as builder

WORKDIR /mnt

COPY . /mnt

RUN apt-get update && apt-get install -y zip

RUN zip -r dependencies.zip jobs \
	-x "*/__pycache__/*" \
	-x "*/tests/*"

### same spark image as our juyter-notebook
FROM jupyter/pyspark-notebook:spark-3.3.1

WORKDIR /workspace

# copy Python dependecies
COPY --from=builder /mnt/dependencies.zip /workspace/dist/dependencies.zip
# copy Spark jobs
COPY ./jobs/ /workspace/jobs/

# provide entrypoint file
COPY ./scripts/entrypoint.sh /workspace/scripts/entrypoint.sh

CMD ["/workspace/scripts/entrypoint.sh"]
