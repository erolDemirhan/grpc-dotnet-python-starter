FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir grpcio==1.64.1 grpcio-tools==1.64.1 fastapi==0.115.0 "uvicorn[standard]==0.30.6" requests==2.32.3

COPY protos/intel.proto /app/protos/intel.proto
RUN python -m grpc_tools.protoc -I/app/protos --python_out=/app --grpc_python_out=/app /app/protos/intel.proto

COPY worker-python/server.py /app/server.py

EXPOSE 50051 8081
ENV NEWSAPI_KEY=
CMD ["python","/app/server.py"]
